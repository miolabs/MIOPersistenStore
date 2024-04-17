//
//  MIOPersistentStore.swift
//  MIOWebServices
//
//  Created by GodShadow on 26/11/2017.
//  Copyright © 2017 MIO Research Labs. All rights reserved.
//

import Foundation
import MIOCore
import MIOCoreData


public protocol MIOPersistentStoreDelegate : NSObjectProtocol
{
    func store(store:MIOPersistentStore, fetchRequest:NSFetchRequest<NSManagedObject>, identifier:UUID?) -> MPSRequest?
    func store(store:MIOPersistentStore, saveRequest:NSSaveChangesRequest) -> MPSRequest?
    
    func store(store: MIOPersistentStore, identifierForObject object:NSManagedObject) -> UUID?
    func store(store: MIOPersistentStore, identifierFromItem item:[String:Any], fetchEntityName: String) -> UUID?
    func store(store: MIOPersistentStore, versionFromItem item:[String:Any], fetchEntityName: String) -> UInt64
}

public enum MIOPersistentStoreError : Error
{
    case noStoreURL(_ schema:String = "", functionName: String = #function)
    case invalidRequest(_ schema:String = "", functionName: String = #function)
    case identifierIsNull(_ schema:String = "", functionName: String = #function)
    case invalidValueType(_ schema:String = "", entityName:String, key:String, value:Any?, functionName: String = #function)
    case relationIdentifierNoExist(_ schema:String = "", entityName:String, relation:String, relationEntityName:String, id:String, functionName: String = #function)
    case delegateIsNull(_ schema:String = "", functionName: String = #function )
}

extension MIOPersistentStoreError: LocalizedError {
    public var errorDescription: String? {
        switch self {
        case let .noStoreURL(schema, functionName):
            return "[MIOPersistentStoreError] \(schema) No store URL. \(functionName)"
        case let .invalidRequest(schema, functionName):
            return "[MIOPersistentStoreError] \(schema) Invalid request. \(functionName)"
        case let .identifierIsNull(schema, functionName):
            return "[MIOPersistentStoreError] \(schema) Identifier is null. \(functionName)"
        case let .invalidValueType(schema, entityName, key, value, functionName):
            return "[MIOPersistentStoreError] \(schema) Invalid value type. \(entityName).\(key): \(value ?? "null"). \(functionName)"
        case let .relationIdentifierNoExist(schema, entityName, relation, relationEntityName, id, functionName):
            return "[MIOPersistentStoreError] \(schema) Relation identifier not exist. \(entityName).\(relation)): \(relationEntityName)://\(id). \(functionName)"
        case let .delegateIsNull(schema, functionName):
            return "[MIOPersistentStoreError] \(schema) Delegate is null. \(functionName)"
        }
    }
}


open class MIOPersistentStore: NSIncrementalStore
{
    //public static var storeType: String { return String(describing: MIOPersistentStore.self) }
    public static var storeType: String { return "MIOPersistentStore" }
    public override var type: String { return MIOPersistentStore.storeType }
    
    public var delegate: MIOPersistentStoreDelegate?
    var storeURL:URL?
    var currentFetchContext:NSManagedObjectContext?
    
    // MARK: - NSIncrementalStore override
    
    public override func loadMetadata() throws {
        
        guard let storeURL = url else {
            throw MIOPersistentStoreError.noStoreURL()
        }
        
        self.storeURL = storeURL
        let metadata = [NSStoreUUIDKey: storeURL.absoluteString, NSStoreTypeKey: MIOPersistentStore.storeType]
        self.metadata = metadata
    }
    
    public override func execute(_ request: NSPersistentStoreRequest, with context: NSManagedObjectContext?) throws -> Any {
        
        switch request {
            
        case let fetchRequest as NSFetchRequest<NSManagedObject>:
            let obs = try fetchObjects(fetchRequest: fetchRequest, with: context!)
            return obs
            
        case let saveRequest as NSSaveChangesRequest:
            try saveObjects(request: saveRequest, with: context!)
            return NSNull()
            
        default:
            throw MIOPersistentStoreError.invalidRequest()
        }
    }
    
    public override func newValuesForObject(with objectID: NSManagedObjectID, with context: NSManagedObjectContext) throws -> NSIncrementalStoreNode {
        
        let identifier = UUID(uuidString: referenceObject(for: objectID) as! String )!
        
        var node = try cacheNode( withIdentifier: identifier, entity: objectID.entity )
        if node == nil {
            node = try cacheNode(newNodeWithValues: [:], identifier: identifier, version: 0, entity: objectID.entity, objectID: objectID)
        }
        
        if node!.version == 0 {
            try fetchObject( withIdentifier:identifier, entityName: objectID.entity.name!, context:context )
        }
        
        let storeNode = try node!.storeNode()
        return storeNode
    }
    
    public override func newValue(forRelationship relationship: NSRelationshipDescription, forObjectWith objectID: NSManagedObjectID, with context: NSManagedObjectContext?) throws -> Any {
        
        let identifier = UUID( uuidString: referenceObject(for: objectID) as! String )!
        
        var node = try cacheNode( withIdentifier: identifier, entity: objectID.entity )
        if node == nil {
            node = try cacheNode(newNodeWithValues: [:], identifier: identifier, version: 0, entity: objectID.entity, objectID: objectID)
        }
        
        if node!.version == 0 {
            try fetchObject( withIdentifier:identifier, entityName: objectID.entity.name!, context:context! )
        }
        
        let value = try node!.value( forRelationship: relationship )
        
        if relationship.isToMany == false {
            guard let relIdentifier = value as? UUID else { return NSNull() }
            
            var relNode = try cacheNode( withIdentifier: relIdentifier, entity: relationship.destinationEntity! )
            if relNode == nil {
                try fetchObject( withIdentifier:relIdentifier, entityName: relationship.destinationEntity!.name!, context:context! )
                relNode = try cacheNode(withIdentifier: relIdentifier, entity: relationship.destinationEntity!)
            }
            
            if relNode == nil {
                let delegate = ( context!.persistentStoreCoordinator!.persistentStores[0] as! MIOPersistentStore ).delegate!
                print("FATAL: CD CACHE NODE NULL: \(delegate): \(objectID.entity.name!).\(relationship.name) -> \(relationship.destinationEntity!.name!)://\(relIdentifier)")
                throw MIOPersistentStoreError.identifierIsNull()
            }
            
            if relNode!.version == 0 {
                try fetchObject( withIdentifier:relIdentifier, entityName: relationship.destinationEntity!.name!, context:context! )
            }
            
            return relNode!.objectID
        }
        else {
            if value is Set<NSManagedObject> {
                return (value as! Set<NSManagedObject>).map{ $0.objectID }
            }
            
            guard let relIdentifiers = value as? [UUID] else {
                return [UUID]()
            }
            
            var objectIDs:Set<NSManagedObjectID> = Set()
            var faultNodeIDs:[UUID] = []
            for relID in relIdentifiers {
                let relNode = try cacheNode( withIdentifier: relID, entity: relationship.destinationEntity! )
                if relNode == nil || relNode?.version == 0 { faultNodeIDs.append( relID ) }
                else { objectIDs.insert( relNode!.objectID ) }
            }
            
            if faultNodeIDs.isEmpty == false {
                try fetchObjects(identifiers: faultNodeIDs, entityName: relationship.destinationEntity!.name!, context: context!)
                for relID in faultNodeIDs {
                    let relNode = try cacheNode(withIdentifier: relID, entity: relationship.destinationEntity!)
                    if relNode == nil {
                        let delegate = (context!.persistentStoreCoordinator!.persistentStores[0] as! MIOPersistentStore ).delegate!
                        print ("FATAL: CD CACHE NODE NULL: \(delegate): \(objectID.entity.name!).\(relationship.name) -> \(relationship.destinationEntity!.name!)://\(relID)")
                        throw MIOPersistentStoreError.identifierIsNull()
                    }
                    
                    objectIDs.insert(relNode!.objectID)
                }
            }
            
            return Array( objectIDs )
        }
    }
    
    public func storedValues(forRelationship relationship: NSRelationshipDescription, forObjectWith objectID: NSManagedObjectID, with context: NSManagedObjectContext?) throws -> [Any]
    {
        let identifier = UUID( uuidString: referenceObject(for: objectID) as! String )!
        
        var node = try cacheNode( withIdentifier: identifier, entity: objectID.entity )
        if node == nil {
            node = try cacheNode(newNodeWithValues: [:], identifier: identifier, version: 0, entity: objectID.entity, objectID: objectID)
        }
        
        if node!.version == 0 {
            try fetchObject( withIdentifier:identifier, entityName: objectID.entity.name!, context:context! )
        }
        
        let value = try node!.value( forRelationship: relationship )

        if let set = value as? Set<NSManagedObject> {
            return set.map{ $0 }
        }
        
        if let uuids = value as? [UUID] {
            return uuids
        }

        return []
    }
    
    public override func obtainPermanentIDs(for array: [NSManagedObject]) throws -> [NSManagedObjectID] {
        return try array.map{ obj in
            guard let identifier = delegate?.store(store: self, identifierForObject: obj) else {
                throw MIOPersistentStoreError.identifierIsNull()
            }
            
            let objID = newObjectID( for: obj.entity, referenceObject: identifier.uuidString.uppercased() )
            
            return objID
        }
    }
    
    public override func managedObjectContextDidRegisterObjects(with objectIDs: [NSManagedObjectID]) {
        
    }
    
    public override func managedObjectContextDidUnregisterObjects(with objectIDs: [NSManagedObjectID]) {
        
        for objID in objectIDs {
            guard let identifier = referenceObject(for: objID) as? String else { continue }
            try? cacheNode( deleteNodeAtIdentifier: UUID( uuidString: identifier )!, entity: objID.entity )
        }
    }
    
    // MARK: - Cache Nodes in memory
    var objectsByEntityName = NSMutableDictionary()
    var nodesByReferenceID = [String:MPSCacheNode]()
    
    let bundleIdentfier = Bundle.main.bundleIdentifier
//    let cacheNodeQueue = DispatchQueue(label: "\(String(describing: Bundle.main.bundleIdentifier)).mws.cache-queue")
    
    func cacheNodeQueue() throws -> DispatchQueue {
        guard let schema = storeURL?.absoluteString else {
            throw MIOPersistentStoreError.noStoreURL()
        }
        
        return MIOCoreQueue(label: "mps.\(schema)" )
    }
    
    func cacheNode(withIdentifier identifier:UUID, entity:NSEntityDescription) throws -> MPSCacheNode? {
        
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        var node:MPSCacheNode?
        try cacheNodeQueue().sync {
            node = nodesByReferenceID[referenceID]
        }
        return node
    }
    
    func cacheNode(newNodeWithValues values:[String:Any], identifier: UUID, version:UInt64, entity:NSEntityDescription, objectID:NSManagedObjectID?) throws -> MPSCacheNode {
            
        let id = identifier
        let objID = objectID ?? newObjectID( for: entity, referenceObject: id.uuidString.uppercased() )
        let node = MPSCacheNode( identifier:id, entity: entity, withValues: values, version: version, objectID: objID )
                               
        try cacheNodeQueue().sync {
            nodesByReferenceID[node.referenceID] = node
        }
        
        if entity.superentity != nil {
            try cacheParentNode( node: node, identifier: identifier, entity: entity.superentity! )
        }
        
        return node
    }
    
    func cacheParentNode(node: MPSCacheNode, identifier: UUID, entity:NSEntityDescription) throws {
                
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
                
        try cacheNodeQueue().sync {
            nodesByReferenceID[referenceID] = node
        }
        
        if entity.superentity != nil {
            try cacheParentNode(node: node, identifier: identifier, entity: entity.superentity!)
        }
    }
    
    func cacheNode(updateNodeWithValues values:[String:Any], identifier:UUID, version:UInt64, entity:NSEntityDescription) throws {
        
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
                
        try cacheNodeQueue().sync {
            let node = nodesByReferenceID[referenceID]
            node?.update(withValues: values, version: version)
        }
    }
    
    func cacheNode(deleteNodeAtIdentifier identifier:UUID, entity:NSEntityDescription) throws {
        let id = identifier
        let referenceID = MPSCacheNode.referenceID(withIdentifier: id, entity: entity)
                
        _ = try cacheNodeQueue().sync {
            nodesByReferenceID.removeValue(forKey: referenceID)
        }
        
        if entity.superentity != nil {
            try cacheNode(deleteNodeAtIdentifier: id, entity: entity.superentity!)
        }
    }
    
    func cacheNode(deletingNodeAtIdentifier identifier:UUID, entity:NSEntityDescription) throws -> Bool {
        let id = identifier
        var deleting = false
        let referenceID = MPSCacheNode.referenceID(withIdentifier: id, entity: entity)
        
        try cacheNodeQueue().sync {
            deleting = deletedObjects.contains(referenceID)
        }
        
        if deleting == true {
            return true
        }
        
        if entity.superentity != nil {
            deleting = try cacheNode(deletingNodeAtIdentifier: id, entity: entity.superentity!)
        }
        
        return deleting
    }
        
    public func refresh(object: NSManagedObject, context: NSManagedObjectContext) throws {
        
        let identifier = UUID(uuidString: object.objectID._referenceObject as! String )!
        let node = try cacheNode(withIdentifier: identifier, entity: object.entity)
        if node != nil { node!.invalidate() }
        
        try fetchObject( withIdentifier: identifier, entityName: object.objectID.entity.name!, context: context )
    }
        
    // MARK: - Fetching objects from server and cache
        
    @discardableResult
    func fetchObject(withIdentifier identifier:UUID, entityName:String, context:NSManagedObjectContext) throws -> Any? {
        return try fetchObjects( identifiers: [identifier], entityName: entityName, context: context )
    }
    
    @discardableResult func fetchObjects(identifiers:[UUID], entityName:String, context:NSManagedObjectContext) throws -> Any? {
        let r = NSFetchRequest<NSManagedObject>(entityName: entityName)
        r.entity = persistentStoreCoordinator?.managedObjectModel.entitiesByName[entityName]
        r.predicate = MIOPredicateWithFormat(format: "identifier in \(identifiers)")
        return try fetchObjects(fetchRequest:r, with:context)
    }
    
    public func fetchObjects(fetchRequest:NSFetchRequest<NSManagedObject>, with context:NSManagedObjectContext) throws -> [Any] {
        
        if delegate == nil {
            throw MIOPersistentStoreError.delegateIsNull( storeURL!.absoluteString )
        }
        
        guard let request = delegate?.store(store: self, fetchRequest: fetchRequest, identifier: nil) as? MPSFetchRequest else {
            throw MIOPersistentStoreError.invalidRequest()
        }
        
        try request.execute()
        
        let object_ids = try updateObjects(items: request.resultItems!, for: fetchRequest.entity!, relationships: request.includeRelationships )
        
        switch fetchRequest.resultType {
        case .managedObjectIDResultType: return object_ids.0
        case .managedObjectResultType  : return try object_ids.0.map{ try context.existingObject(with: $0) }
        default: return []
        }
    }
    
    
    // MARK: -  Saving objects in server and caché
    func saveObjects(request:NSSaveChangesRequest, with context:NSManagedObjectContext) throws {
        let request = self.delegate?.store(store: self, saveRequest: request)
        try request?.execute()
        
        // TODO: update cache
    }


    let deletedObjects = NSMutableSet()


    func versionForItem(_ values: [String:Any], entityName: String) -> UInt64 {
        guard let version = delegate?.store(store: self, versionFromItem: values, fetchEntityName: entityName) else {
            return 1
        }
        
        return version
    }
    
    func identifierForItem(_ values: [String:Any], entityName:String) -> UUID? {
        return delegate?.store(store: self, identifierFromItem: values, fetchEntityName: entityName)
    }
    
}

