//
//  MIOPersistentStore.swift
//  MIOWebServices
//
//  Created by GodShadow on 26/11/2017.
//  Copyright © 2017 MIO Research Labs. All rights reserved.
//

import Foundation

#if APPLE_CORE_DATA
import CoreData
#else
import MIOCoreData
#endif


public protocol MIOPersistentStoreDelegate : NSObjectProtocol
{
    //func store(_ store:MWSPersistentStore, requestForEntityName entityName:String, url:URL, bodyData:Data?, httpMethod:String) -> MWSRequest
    
    func store(store:MIOPersistentStore, fetchRequest:NSFetchRequest<NSManagedObject>, serverID:String?) -> MPSRequest?
    func store(store: MIOPersistentStore, insertRequestForObject object: NSManagedObject, dependencyIDs:inout [String]) -> MPSRequest?
    func store(store: MIOPersistentStore, updateRequestForObject object: NSManagedObject, dependencyIDs:inout [String]) -> MPSRequest?
    func store(store: MIOPersistentStore, deleteRequestForObject object: NSManagedObject) -> MPSRequest?
    
    func store(store: MIOPersistentStore, identifierForObject object:NSManagedObject) -> UUID?
    func store(store: MIOPersistentStore, identifierFromItem item:[String:Any], fetchEntityName: String) -> String?
    func store(store: MIOPersistentStore, versionFromItem item:[String:Any], fetchEntityName: String) -> UInt64
    
    //
    //    func webStore(store: MWSPersistentStore, requestDidFinishWithResult result:Bool, code:Int, data:Any?) -> MWSRequestResponse
    //
    
    
    //    func store(store: MIOPersistentStore, entityNameFromItem item:[String:Any], fetchEntityName: String) -> String
    
    //
    //
    //    func store(store: MIOPersistentStore, tableKeyForAttributeName attributeName: String, forEntity entity: NSEntityDescription) -> String
    //    func store(store: MIOPersistentStore, tableValueForAttribute attribute: NSAttributeDescription, value:Any?) -> Any?
    //
    //    func store(store: MIOPersistentStore, tableKeyForRelationshipName relationshipName: String, forEntity entity: NSEntityDescription) -> String
    //
    
    //    //func webStore(store: MWSPersistentStore, serverValue:Any, for attribute: NSAttributeDescription) -> Any
    //
    //    func webStore(store: MWSPersistentStore, serverIDForCachedItem item:Any, entityName: String) -> String
    //
    //    func mainContextForWebStore(store: MWSPersistentStore) -> NSManagedObjectContext
}

public enum MIOPersistentStoreRequestType
{
    case Fetch
    case Insert
    case Update
    case Delete
}

public enum MIOPersistentStoreError : Error
{
    case noStoreURL
    case invalidRequest
    case identifierIsNull
}

public enum MIOPersistentStoreConnectionType
{
    case Synchronous
    case ASynchronous
}

open class MIOPersistentStore: NSIncrementalStore
{
    //public static var storeType: String { return String(describing: MIOPersistentStore.self) }
    public static var storeType: String { return "MIOPersistentStore" }
    
    public override var type: String { return MIOPersistentStore.storeType }
    
    public var delegate: MIOPersistentStoreDelegate?
    
    open var connectionType = MIOPersistentStoreConnectionType.Synchronous
    
    var storeURL:URL?
    
    //    public func fetchRequest(withEntityName entityName:String, context:NSManagedObjectContext) -> NSFetchRequest<NSFetchRequestResult>{
    //        let request = NSFetchRequest<NSFetchRequestResult>()
    //        let entity = NSEntityDescription.entity(forEntityName: entityName, in: context)
    //        request.entity = entity
    //
    //        return request
    //    }
    
    var currentFetchContext:NSManagedObjectContext?
    //    public func performFetchInFetchedResultsController(fetchResultsController:NSFetchedResultsController<NSFetchRequestResult>) throws {
    //        currentFetchContext = fetchResultsController.managedObjectContext
    //        try fetchResultsController.performFetch()
    //        currentFetchContext = nil
    //    }
    
    // MARK: - NSPersistentStore override
    
//    public required init(persistentStoreCoordinator root: NSPersistentStoreCoordinator?, configurationName name: String?, at url: URL, options: [AnyHashable : Any]? = nil) {
//            super.init(persistentStoreCoordinator: root, configurationName: name, at: url, options: options)
//    }
    
    public override func loadMetadata() throws {
        
        guard let storeURL = url else {
            throw MIOPersistentStoreError.noStoreURL
        }
        
        self.storeURL = storeURL
        let metadata = [NSStoreUUIDKey: UUID().uuidString, NSStoreTypeKey: MIOPersistentStore.storeType]
        self.metadata = metadata
    }
    
    public override func execute(_ request: NSPersistentStoreRequest, with context: NSManagedObjectContext?) throws -> Any {
        
        switch request {
        
        case let fetchRequest as NSFetchRequest<NSManagedObject>:
            let obs = try fetchObjects(fetchRequest: fetchRequest, with: context!)
            return obs
            
        case let saveRequest as NSSaveChangesRequest:
            try saveObjects(request: saveRequest, with: context!)
            return []
            
        default:
            throw MIOPersistentStoreError.invalidRequest
        }
    }
    
    // MARK: - NSIncrementalStore override
    
    public override func newValuesForObject(with objectID: NSManagedObjectID, with context: NSManagedObjectContext) throws -> NSIncrementalStoreNode {
        
        let identifier = referenceObject(for: objectID) as! String
        let node = cacheNode(withIdentifier: identifier, entity: objectID.entity) ??
                   cacheNode(newNodeWithValues: [:], identifier: identifier, version: 0, entity: objectID.entity, objectID: objectID)
        
        if (node.version == 0) {
            try checkForDerivated( node, objectID.entity.name!, identifier, context )
        }
        
        let storeNode = try node.storeNode()
        return storeNode
    }

    
    func checkForDerivated ( _ node: MPSCacheNode, _ entityName: String, _ identifier: String, _ context: NSManagedObjectContext ) throws {
        try fetchObject(With:identifier, entityName: entityName, context:context)
//
//        let derivatedEntity = node._values[ "classname" ] as? String
//
//        if derivatedEntity != nil && derivatedEntity! != entityName {
//            try fetchObject(With:identifier, entityName: derivatedEntity!, context:context)
//        }
    }

    
    public override func newValue(forRelationship relationship: NSRelationshipDescription, forObjectWith objectID: NSManagedObjectID, with context: NSManagedObjectContext?) throws -> Any {
        
        let identifier = referenceObject(for: objectID) as! String

        let node = (cacheNode(withIdentifier: identifier, entity: objectID.entity))!
        if node.version == 0 {
            try checkForDerivated( node, objectID.entity.name!, identifier, context! )
            // try fetchObject(With:identifier, entityName: objectID.entity.name!, context:context!)
        }
        
        let value = try node.value(forRelationship: relationship)
        
        if relationship.isToMany == false {
            guard let relIdentifier = value as? String else {
                return NSNull()
            }
            
            var relNode = cacheNode(withIdentifier: relIdentifier, entity: relationship.destinationEntity!)
            if relNode == nil {
                relNode = cacheNode(newNodeWithValues: [:],  identifier: relIdentifier, version: 0, entity:relationship.destinationEntity!, objectID: nil)
                // try fetchObject(With:relIdentifier, entityName: relationship.destinationEntity!.name!, context:context!)
                try checkForDerivated( relNode!, relationship.destinationEntity!.name!, relIdentifier, context! )

            }
            
            return relNode!.objectID
        }
        else {
            guard let relIdentifiers = value as? [String] else {
                return []
            }
            
            var array:[NSManagedObjectID] = []
            for relID in relIdentifiers {
                var relNode = cacheNode(withIdentifier: relID, entity: relationship.destinationEntity!)
                if relNode == nil {
                    relNode = cacheNode(newNodeWithValues: [:], identifier: relID, version: 0, entity: relationship.destinationEntity!, objectID: nil)
                    // try fetchObject(With:relID, entityName: relationship.destinationEntity!.name!, context:context!)
                    try checkForDerivated( relNode!, relationship.destinationEntity!.name!, relID, context! )
                }
                array.append(relNode!.objectID)
            }
            
            return array
        }
    }
    
    public override func obtainPermanentIDs(for array: [NSManagedObject]) throws -> [NSManagedObjectID] {
        return try array.map{ obj in
            guard let identifier = delegate?.store(store: self, identifierForObject: obj) else {
                throw MIOPersistentStoreError.identifierIsNull
            }
            
            let objID = newObjectID( for: obj.entity, referenceObject: identifier.uuidString )
            
            return objID
        }
    }
    
    public override func managedObjectContextDidRegisterObjects(with objectIDs: [NSManagedObjectID]) {
        
    }
    
    public override func managedObjectContextDidUnregisterObjects(with objectIDs: [NSManagedObjectID]) {
        
    }
    
    // MARK: - Cache Nodes in memory
    var objectsByEntityName = NSMutableDictionary()
    var nodesByReferenceID = [String:MPSCacheNode]()
    
    let bundleIdentfier = Bundle.main.bundleIdentifier
    let cacheNodeQueue = DispatchQueue(label: "\(String(describing: Bundle.main.bundleIdentifier)).mws.cache-queue")
    
    func cacheNode(withIdentifier identifier:String, entity:NSEntityDescription) -> MPSCacheNode? {
                        
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        print("CacheNode Query: \(referenceID)")
        var node:MPSCacheNode?
        cacheNodeQueue.sync {
            node = nodesByReferenceID[referenceID]
        }
        return node
    }
    
    func cacheNode(newNodeWithValues values:[String:Any], identifier: String, version:UInt64, entity:NSEntityDescription, objectID:NSManagedObjectID?) -> MPSCacheNode {
            
        let objID = objectID ?? newObjectID(for: entity, referenceObject: identifier)
        //let node = NSIncrementalStoreNode(objectID: objID, withValues: values, version: version)
        let node = MPSCacheNode(identifier:identifier, entity: entity, withValues: values, version: version, objectID: objID)
        
        print("CacheNode Insert: \(node.referenceID)")
        
        cacheNodeQueue.sync {
            nodesByReferenceID[node.referenceID] = node
            //            if enableLog {
            //                NSLog("Inserting REFID: " + referenceID);
            //            }
        }
        
        if entity.superentity != nil {
            cacheParentNode(node: node, identifier: identifier, entity: entity.superentity!)
        }
        
        return node
    }
    
    func cacheParentNode(node: MPSCacheNode, identifier: String, entity:NSEntityDescription) {
                
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        
        print("CacheNode Insert: \(referenceID)")
        
        cacheNodeQueue.sync {
            nodesByReferenceID[referenceID] = node
            //            if enableLog {
            //                NSLog("Inserting Parent REFID: " + referenceID);
            //            }
        }
        
        if entity.superentity != nil {
            cacheParentNode(node: node, identifier: identifier, entity: entity.superentity!)
        }
    }
    
    func cacheNode(updateNodeWithValues values:[String:Any], identifier:String, version:UInt64, entity:NSEntityDescription) {
        
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        
        print("CacheNode Update: \(referenceID)")
        
        cacheNodeQueue.sync {
            let node = nodesByReferenceID[referenceID]
            node?.update(withValues: values, version: version)
            //self.updateRelationshipIDs(oldRelationshipIDs: relationshipValuesByReferenceID[referenceID], withNewRelationshipIDs: values["relationshipIDs"])
            //            if enableLog {
            //                NSLog("Updating REFID: " + referenceID)
            //            }
        }
    }
    
    func cacheNode(deleteNodeAtIdentifier identifier:String, entity:NSEntityDescription) {
        
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        print("CacheNode Delete: \(referenceID)")
        
        _ = cacheNodeQueue.sync {
            nodesByReferenceID.removeValue(forKey: referenceID)
            //            relationshipValuesByReferenceID.removeObject(forKey: referenceID)
            //            if enableLog {
            //                NSLog("Delete REFID: " + referenceID)
            //            }
        }
        
        if entity.superentity != nil {
            cacheNode(deleteNodeAtIdentifier: identifier, entity: entity.superentity!)
        }
    }
    
    func cacheNode(deletingNodeAtIdentifier identifier:String, entity:NSEntityDescription) -> Bool {
        
        var deleting = false
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifier, entity: entity)
        cacheNodeQueue.sync {
            deleting = deletedObjects.contains(referenceID)
        }
        
        if deleting == true {
            return true
        }
        
        if entity.superentity != nil {
            deleting = cacheNode(deletingNodeAtIdentifier: identifier, entity: entity.superentity!)
        }
        
        return deleting
    }
    
    
    lazy var operationQueue : OperationQueue = {
        return OperationQueue()
    }()
    
    // MARK: - Fetching objects from server and cache
    
    var fetchingObjects = [String : Bool]()
    @discardableResult func fetchObject(With serverID:String, entityName:String, context:NSManagedObjectContext) throws -> Any? {
        
        let fetchRequest = NSFetchRequest<NSManagedObject>(entityName: entityName)
        fetchRequest.entity = persistentStoreCoordinator?.managedObjectModel.entitiesByName[entityName]
        //request.predicate = NSPredicate(format: "identifier == '\(serverID)'")
        
        guard let request = delegate?.store(store: self, fetchRequest: fetchRequest, serverID: serverID) else {
            return nil
        }
        
        let op = MPSFetchOperation(store:self, request:request, entity:fetchRequest.entity!, relationshipKeyPathsForPrefetching:fetchRequest.relationshipKeyPathsForPrefetching, identifier: nil)
        op.completionBlock = {
            
            context.performAndWait {
                for objID in op.insertedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
                
                for objID in op.updatedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
            }
            
            //            if completion != nil {
            //                completion!(op.objectIDs, op.insertedObjectIDs, op.updatedObjectIDs)
            //            }
        }
        
        op.moc = context
        operationQueue.addOperation(op)
        
        if connectionType == .ASynchronous { return [] }
        
        operationQueue.waitUntilAllOperationsAreFinished()
        
        if fetchRequest.resultType == .managedObjectIDResultType { return op.objectIDs }
        
        if fetchRequest.resultType == .managedObjectResultType {
            
            var objs:[NSManagedObject] = []
            for objID in op.objectIDs {
                let obj = try context.existingObject(with: objID)
                objs.append(obj)
            }
            return objs
        }
        
        return nil
        
        //        if delegate == nil {
        //            return
        //        }
        //
        //        if (fetchingObjects[serverID] != nil) {
        //            return
        //        }
        //
        //        fetchingObjects[serverID] = true;
        //        if enableLog {
        //            NSLog("Downloading REFID: " + serverID);
        //        }
    }
    
    public func fetchObjects(fetchRequest:NSFetchRequest<NSManagedObject>, with context:NSManagedObjectContext) throws -> [Any] {
        
        if delegate == nil {
            return []
        }
        
        //        if currentFetchContext != nil {
        //            contextByRequest[fetchRequest] = currentFetchContext
        //        }
        
        guard let request = delegate?.store(store: self, fetchRequest: fetchRequest, serverID: nil) else {
            return []
        }
        
        let op = MPSFetchOperation(store:self, request:request, entity:fetchRequest.entity!, relationshipKeyPathsForPrefetching:fetchRequest.relationshipKeyPathsForPrefetching, identifier: nil)
        op.completionBlock = {
            
            context.performAndWait {
                for objID in op.insertedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
                
                for objID in op.updatedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
            }
            
            //            if completion != nil {
            //                completion!(op.objectIDs, op.insertedObjectIDs, op.updatedObjectIDs)
            //            }
        }
        op.moc = context
        
        operationQueue.addOperation(op)
        //}
        
        //                guard let set = objectsByEntityName[fetchRequest.entityName!] as? NSMutableSet else {
        //                    return []
        //                }
        //
        //                let realObjectsSet = NSMutableSet()
        //                for objID in set {
        //                    let obj = context.object(with: objID as! NSManagedObjectID)
        //                    realObjectsSet.add(obj)
        //
        ////            Code for debugging strange *invalid relationships* bug (isTemporaryID crash because bad managed object)
        ////            let mo = obj as! NSManagedObject
        ////            let v = mo.value(forKeyPath: "note.id")
        ////            print("noteid: \(String(describing: v))")
        //        }
        //
        //        var predicateSet:NSSet
        //        if fetchRequest.predicate != nil {
        //            predicateSet = realObjectsSet.filtered(using: fetchRequest.predicate!) as NSSet
        //        }
        //        else {
        //            predicateSet = realObjectsSet
        //        }
        //
        //        var objects:[Any]
        //
        //        if let sds = fetchRequest.sortDescriptors {
        //            objects = predicateSet.sortedArray(using: sds)
        //        } else {
        //            objects = predicateSet.allObjects;
        //        }
        //
        //        if fetchRequest.resultType == .managedObjectResultType {
        //            return objects
        //        }
        //
        //        var ids = [Any]()
        //        for obj in objects {
        //            let o = obj as! NSManagedObject
        //            ids.append(o.objectID)
        //        }
        //
        //        return ids
        if connectionType == .ASynchronous { return [] }

        operationQueue.waitUntilAllOperationsAreFinished()

        switch fetchRequest.resultType {
            case .managedObjectIDResultType:
                return op.objectIDs
            case .managedObjectResultType:
                return try op.objectIDs.map{ try context.existingObject(with: $0) }
            default:
                return []
        }
    }
    
    func cacheObjectForContext(objID:NSManagedObjectID, entity:NSEntityDescription, context:NSManagedObjectContext, refresh:Bool) {
        
        do {
            let obj = try context.existingObject(with: objID)
            if refresh == true  {
                context.refresh(obj, mergeChanges: true)
            }
            var set = self.objectsByEntityName[entity.name!] as? NSMutableSet
            if set == nil {
                set = NSMutableSet()
            }
            set?.add(obj.objectID)
            self.objectsByEntityName[entity.name!] = set
            
            if let superentity = entity.superentity {
                let identifier = referenceObject(for: objID) as! String
                let node = cacheNode(withIdentifier: identifier, entity: superentity)
                cacheObjectForContext(objID: node!.objectID, entity:superentity, context: context, refresh: refresh)
            }
        }
        catch {
            print("ERROR: \(error)")
        }
        
        
    }
    
    func deleteCacheObjectForContext(objID:NSManagedObjectID, entity:NSEntityDescription, context:NSManagedObjectContext){
        
        let set = self.objectsByEntityName[entity.name!] as? NSMutableSet
        if set == nil {
            return
        }
        set?.remove(objID)
        self.objectsByEntityName[entity.name!] = set
        
        if let superentity = entity.superentity {
            deleteCacheObjectForContext(objID: objID, entity:superentity, context: context)
        }
    }
    
    
    // MARK: -  Saving objects in server and caché
    var saveCount = 0
    lazy var saveOperationQueue : OperationQueue = {
        var queue = OperationQueue()
        
        queue.maxConcurrentOperationCount = 1
        
        return queue
    }()
    
    func saveObjects(request:NSSaveChangesRequest, with context:NSManagedObjectContext) throws {
        var insertError: Error?
        
        try request.insertedObjects?.forEach({ (obj) in
            if obj.changedValues().count > 0 {
                try insertObjectIntoServer(object: obj, context: context, onError: { insertError = $0 } )
                //insertObjectIntoCache(object: obj)
            }
        })
        
        try request.updatedObjects?.forEach({ (obj) in
            if obj.changedValues().count > 0 {
                try updateObjectOnServer(object: obj, context: context)
                //updateObjectOnCache(object: obj)
            }
        })
        
        try request.deletedObjects?.forEach({ (obj) in
            try deleteObjectOnServer(object: obj, context: context)
            //deleteObjectOnCache(object: obj)
            //            let serverID = referenceObject(for: obj.objectID) as! String
            //            let referenceID = obj.entity.name! + "://" + serverID;
            //            relationshipValuesByReferenceID.removeObject(forKey: referenceID)
        })
        
        uploadToServer()
        saveCount += 1;
        
        if connectionType == .Synchronous {
            saveOperationQueue.waitUntilAllOperationsAreFinished()
            if insertError != nil { throw insertError! }
        }
    }
    
    func insertObjectIntoServer(object:NSManagedObject, context:NSManagedObjectContext, onError: @escaping ( _ error: Error ) -> Void ) throws {
        
        guard let identifier = delegate?.store(store: self, identifierForObject: object) else {
            throw MIOPersistentStoreError.identifierIsNull
        }
        
        let identifierString = identifier.uuidString
        
        //let attribValues = self.filterOnlyAttributes(fromAllValuesOfManagedObject: object)
        
        _ = cacheNode(newNodeWithValues: object.changedValues(), identifier: identifierString, version: 1, entity: object.entity, objectID: object.objectID)
        
        cacheObjectForContext(objID: object.objectID, entity:object.entity, context: context, refresh: false)
        
        var dependencies:[String] = []
        guard let request = delegate?.store(store: self, insertRequestForObject: object, dependencyIDs:&dependencies) else {
            return
        }
        
        let op = MPSInsertOperation(store:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil, identifier: nil)
        //op.webStoreCache = persistentStoreCache
        op.completionBlock = {
            if op.responseError != nil {
                onError( op.responseError! )
                self.operationQueue.cancelAllOperations()
            }
            else {
                context.performAndWait {
                    for objID in op.insertedObjectIDs {
                        self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh:true)
                    }
                    
                    for objID in op.updatedObjectIDs {
                        self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
                    }
                }
                
                if op.responseCode != 200 {
                    NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
                }
            }
        }
        
        op.serverID = identifierString
        op.moc = context
        op.dependencyIDs = dependencies
        op.saveCount = saveCount
        addOperation(operation: op, serverID:identifierString)
    }
    
    func updateObjectOnServer(object:NSManagedObject, context:NSManagedObjectContext) throws {
        
        guard let identifier = delegate?.store(store: self, identifierForObject: object) else {
            throw MIOPersistentStoreError.identifierIsNull
        }
        
        let identifierString = identifier.uuidString

        let node = (cacheNode(withIdentifier: identifierString, entity: object.entity))!
        
        //let attribValues = self.filterOnlyAttributes(fromAllValuesOfManagedObject: object)
        
        cacheNode(updateNodeWithValues: object.changedValues(), identifier: identifierString, version: node.version + 1, entity: object.entity)
        
        var dependencies:[String] = []
        guard let request = delegate?.store(store: self, updateRequestForObject: object, dependencyIDs: &dependencies) else {
            return
        }
                
        let op = MPSUpdateOperation(store:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil, identifier: nil)
        //op.webStoreCache = persistentStoreCache
        op.nodeVersion = node.version
        op.completionBlock = {
            context.performAndWait {
                for objID in op.insertedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
                }
                
                for objID in op.updatedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
                }
            }
            
            if op.responseCode != 200 {
                NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
            }
            
        }
        
        op.serverID = identifierString
        op.moc = context
        op.dependencyIDs = dependencies
        op.saveCount = saveCount
        addOperation(operation: op, serverID:identifierString)
    }
    
    let deletedObjects = NSMutableSet()
    func deleteObjectOnServer(object:NSManagedObject, context:NSManagedObjectContext) throws {
        
        guard let identifier = delegate?.store(store: self, identifierForObject: object) else {
            throw MIOPersistentStoreError.identifierIsNull
        }
        let identifierString = identifier.uuidString

        cacheNode(deleteNodeAtIdentifier: identifierString, entity: object.entity)
        let referenceID = MPSCacheNode.referenceID(withIdentifier: identifierString, entity: object.entity)
        deletedObjects.add(referenceID)
        self.deleteCacheObjectForContext(objID: object.objectID, entity: object.entity, context: context)
        
        guard let request = delegate?.store(store: self, deleteRequestForObject: object) else {
            return
        }
        
        let op = MPSDeleteOperation(store:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil, identifier: nil)
        //op.webStoreCache = persistentStoreCache
        op.completionBlock = {
            context.performAndWait {
                for objID in op.insertedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
                
                for objID in op.updatedObjectIDs {
                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
                }
                
            }
            if op.responseCode == 200 {
                //TODO PARCHACO BORRADOS
                /*self.cacheNodeQueue.async {
                 self.deletedObjects.remove(referenceID)
                 }*/
            }
            else if op.responseCode != 200 {
                NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
            }
        }
        
        op.serverID = identifierString
        op.moc = context
        addOperation(operation: op, serverID: identifierString)
    }
    
    var saveOperationsByReferenceID = [String:MPSPersistentStoreOperation]()
    var uploadingOperations = [String:Any]()
    
    func addOperation(operation:MPSPersistentStoreOperation, serverID:String){
        saveOperationsByReferenceID[serverID] = operation
    }
    
    func removeOperation(operation:MPSPersistentStoreOperation, serverID:String){
        saveOperationsByReferenceID[serverID] = nil
    }
    
    func operationAtServerID(serverID:String, saveCount:Int) -> MPSPersistentStoreOperation? {
        return saveOperationsByReferenceID[serverID]
    }
    
    func checkOperationDependecies(operation: MPSPersistentStoreOperation, dependencies:[String]) {
        
        for referenceID in dependencies {
            var op = operationAtServerID(serverID:referenceID, saveCount:saveCount)
            if (op == nil) {
                op = lastUploadingOperationByServerID(serverID:referenceID)
            }
            
            if (op == nil) { continue }
            operation.addDependency(op!)
        }
    }
    
    func uploadToServer() {
        
        // Read from cache to know if there's any pending operation
        //            if (persistentStoreCache != nil) {
        //                let operations = persistentStoreCache!.pendingOperations()
        //                for pendingOperation in operations {
        //                    addUploadingOperation(operation: pendingOperation, serverID: pendingOperation.serverID!)
        //                    saveOperationQueue.addOperation(pendingOperation);
        //                }
        //            }
        
        for (refID, op) in saveOperationsByReferenceID {
            checkOperationDependecies(operation:op, dependencies: op.dependencyIDs)
            addUploadingOperation(operation:op, serverID:refID);
            saveOperationQueue.addOperation(op);
        }
        
        saveOperationsByReferenceID = [String:MPSPersistentStoreOperation]()
    }
    
    func addUploadingOperation(operation:MPSPersistentStoreOperation, serverID:String){
        
        var array = uploadingOperations[serverID] as? NSMutableArray
        if array == nil {
            array = NSMutableArray()
            uploadingOperations[serverID] = array
        }
        else {
            let lastOP = array?.lastObject as! MPSPersistentStoreOperation
            operation.addDependency(lastOP)
        }
        
        array?.add(operation);
    }
    
    func lastUploadingOperationByServerID(serverID:String) -> MPSPersistentStoreOperation? {
        let array = uploadingOperations[serverID] as? NSMutableArray
        if (array == nil) {
            return nil
        }
        if (array!.count == 0) {
            return nil
        }
        return array!.lastObject as? MPSPersistentStoreOperation
    }
    
    /*
    func updateObjects(items:[Any], for entity:NSEntityDescription, relationships:[String]?) -> ([NSManagedObjectID], [NSManagedObjectID], [NSManagedObjectID]) {
        
        let objects = NSMutableSet()
        let insertedObjects = NSMutableSet()
        let updatedObjects = NSMutableSet()
        let relationshipNodes = NSMutableDictionary()
        relationShipsNodes(relationships: relationships, nodes: relationshipNodes)
        
        for i in items {
            let values = i as! [String : Any]
            updateObject(values:values, fetchEntity:entity, objectID:nil, relationshipNodes: relationshipNodes, objectIDs:objects, insertedObjectIDs:insertedObjects, updatedObjectIDs:updatedObjects)
        }
        
        return (objects.allObjects as! [NSManagedObjectID], insertedObjects.allObjects as! [NSManagedObjectID], updatedObjects.allObjects as! [NSManagedObjectID])
    }
    
    func updateObject(values:[String:Any], fetchEntity:NSEntityDescription, objectID:NSManagedObjectID?, relationshipNodes:NSMutableDictionary?, objectIDs:NSMutableSet, insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) {
        
        //        guard let entityName = delegate?.store(store: self, entityNameFromItem:values, fetchEntityName: fetchEntity.name!) else {
        //            return
        //        }
        
        //var entity = fetchEntity
        
        //        if entity.name != entityName {
        //            let ctx = (webStore.delegate?.mainContextForWebStore(store: webStore))!
        //            entity = NSEntityDescription.entity(forEntityName: entityName, in: ctx)!
        //        }
        
        // Check the objects inside values
        let parsedValues = checkRelationships(values:values, entity: fetchEntity, relationshipNodes: relationshipNodes, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
        
        guard let serverID = delegate?.store(store: self, identifierFromItem: parsedValues, fetchEntityName: fetchEntity.name!) else {
            return
        }
        //if (serverID == nil) return nil;
        
        // Check if the server is deleting the object and ignoring
        if cacheNode(deletingNodeAtServerID:serverID, entity:fetchEntity) == true {
            return
        }
        
        guard let version = delegate?.store(store: self, versionFromItem: values, fetchEntityName: fetchEntity.name!) else {
            return
        }
        
        var node = cacheNode(WithServerID: serverID, entity: fetchEntity)
        if node == nil {
            NSLog("New version: " + fetchEntity.name! + " (\(version))");
            node = cacheNode(newNodeWithValues: parsedValues, atServerID: serverID, version: version, entity: fetchEntity, objectID: objectID)
            insertedObjectIDs.add(node!.objectID)
        }
        else if version > node!.version {
            NSLog("Update version: \(fetchEntity.name!) (\(node!.version) -> \(version))")
            cacheNode(updateNodeWithValues: parsedValues, atServerID: serverID, version: version, entity: fetchEntity)
            updatedObjectIDs.add(node!.objectID)
        }
        
        objectIDs.add(node!.objectID)
    }
    
    private func checkRelationships(values : [String : Any], entity:NSEntityDescription, relationshipNodes : NSMutableDictionary?, objectIDs:NSMutableSet, insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) -> [String : Any] {
        
        var parsedValues = [String:Any]()
        let relationshipsIDs = NSMutableDictionary()
        
        for key in entity.propertiesByName.keys {
            
            let prop = entity.propertiesByName[key]
            if prop is NSAttributeDescription {
                
                //guard let serverKey = delegate?.store(store: self, tableKeyForAttributeName: key, forEntity: entity) else { continue }
                let serverKey = key
                
                //let newValue = delegate?.store(store: self, tableValueForAttribute: prop! as! NSAttributeDescription, value:values[serverKey])
                let newValue = values[serverKey]
                
                let attr = prop as! NSAttributeDescription
                
                switch  ( attr.attributeType ) {
                case .dateAttributeType:
                    parsedValues[key] = newValue as? Date
                    
                case .UUIDAttributeType:
                    parsedValues[key] = newValue is String ? UUID(uuidString: newValue as! String ) : nil
                    
                default:
                    if newValue != nil && newValue is NSNull == false {
                        // check type
                        switch attr.attributeType {
                        case .booleanAttributeType, .decimalAttributeType, .doubleAttributeType, .floatAttributeType, .integer16AttributeType, .integer32AttributeType, .integer64AttributeType:
                            assert(newValue is NSNumber, "[Black Magic] Received Number with incorrect type for key \(key)")
                            
                        case .stringAttributeType:
                            assert(newValue is NSString, "[Black Magic] Received String with incorrect type for key \(key)")
                            
                        default:
                            assert(true)
                        }
                    }
                    parsedValues[key] = newValue
                }
            }
            else if prop is NSRelationshipDescription {
                
                //guard let serverKey = delegate?.store(store: self, tableKeyForRelationshipName: key, forEntity: entity) else { continue }
                
                let serverKey = key
                
                if relationshipNodes?[key] == nil {
                    relationshipsIDs[key] = values[serverKey]
                    continue
                }
                
                let relEntity = entity.relationshipsByName[key]!
                let value = values[serverKey]
                if value == nil {
                    continue
                }
                
                if relEntity.isToMany == false {
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    let serverValues = value as? [String:Any]
                    if serverValues != nil {
                        _ = updateObject(values: serverValues!, fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        let serverID = delegate?.store(store: self, identifierFromItem: value as! [String:Any], fetchEntityName: relEntity.destinationEntity!.name!)
                        relationshipsIDs[key] = serverID
                    }
                }
                else {
                    
                    var array = [String]()
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    let serverValues = value as! [Any]
                    for relatedItem in serverValues {
                        _ = updateObject(values: relatedItem as! [String:Any], fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode!, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        let serverID = delegate?.store(store: self, identifierFromItem: relatedItem as! [String:Any], fetchEntityName: relEntity.destinationEntity!.name!)
                        array.append(serverID!)
                    }
                    
                    relationshipsIDs[key] = array
                }
            }
        }
        
        parsedValues["relationshipIDs"] = relationshipsIDs
        return parsedValues
    }
    
    func relationShipsNodes(relationships:[String]?, nodes: NSMutableDictionary) {
        
        if relationships == nil {
            return
        }
        
        for keyPath in relationships! {
            let keys = keyPath.split(separator: ".")
            let key = String(keys[0])
            
            var values = nodes[key] as? NSMutableDictionary
            if values == nil {
                values = NSMutableDictionary()
                nodes[key] = values!
            }
            
            if (keys.count > 1) {
                let index = keyPath.index(keyPath.startIndex, offsetBy:key.count + 1)
                let subKeyPath = String(keyPath[index...])
                //var subNodes = [String:Any]()
                relationShipsNodes(relationships: [subKeyPath], nodes: values!)
                //                values?.merge(subNodes, uniquingKeysWith: { (OldValue, newValue) -> Any in
                //                    return newValue
                //                })
                //                nodes[key] = values!
            }
        }
        
    }
     */
    
    func versionForItem(_ values: [String:Any], entityName: String) -> UInt64 {
        guard let version = delegate?.store(store: self, versionFromItem: values, fetchEntityName: entityName) else {
            return 1
        }
        
        return version
    }
    
    func identifierForItem(_ values: [String:Any], entityName:String) -> String? {
        return delegate?.store(store: self, identifierFromItem: values, fetchEntityName: entityName)
    }
    
}

