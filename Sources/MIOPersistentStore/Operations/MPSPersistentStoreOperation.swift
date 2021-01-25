//
//  MWSPersistentStoreOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

import MIOCore
#if APPLE_CORE_DATA
import CoreData
#else
import MIOCoreData
#endif


class MPSPersistentStoreOperation: Operation
{
    private var _identifier:String!
    var identifier:String {
        get { return _identifier }
    }
    
    var store:MIOPersistentStore
    var request:MPSRequest
    var entity:NSEntityDescription
    var relationshipKeyPathsForPrefetching:[String]
    var serverID:String?
    var moc: NSManagedObjectContext?

    
    var saveCount = 0
    var dependencyIDs:[String] = []
    
    var responseResult = false
    var responseCode:Int = 0
    var responseData:Any?
    var responseError:Error?
            
    var objectIDs = [NSManagedObjectID]()
    var insertedObjectIDs = [NSManagedObjectID]()
    var updatedObjectIDs = [NSManagedObjectID]()
    var deletedObjectIDs = [NSManagedObjectID]()
    
    var requestCount = 0
    
    private var _uploading = false;
    var uploading:Bool {
        set {
            willChangeValue(forKey: "isExecuting")
            _uploading = newValue
            didChangeValue(forKey: "isExecuting")
        }
        get {
            return _uploading
        }
    }

    private var _uploaded = false;
    var uploaded:Bool {
        set {
            willChangeValue(forKey: "isFinished")
            _uploaded = newValue
            didChangeValue(forKey: "isFinished")
        }
        get {
            return _uploaded
        }
    }
    
    override func cancel() {
        willChangeValue(forKey: "isFinished")
        super.cancel()
        didChangeValue(forKey: "isFinished")
    }
        
    init(store:MIOPersistentStore, request:MPSRequest, entity:NSEntityDescription, relationshipKeyPathsForPrefetching:[String]?, identifier:String?) {
        _identifier = identifier ?? UUID().uuidString.lowercased( )
        self.store = store
        self.request = request
        self.entity = entity
        self.relationshipKeyPathsForPrefetching = relationshipKeyPathsForPrefetching ?? [String]()
        super.init()
    }
    
    convenience init(store:MIOPersistentStore, request:MPSRequest, entity:NSEntityDescription, relationshipKeyPathsForPrefetching:[String]?) {
        self.init(store: store, request: request, entity: entity, relationshipKeyPathsForPrefetching: relationshipKeyPathsForPrefetching, identifier: nil)
    }
    
    override func start() {
        
        assert(self.uploading == false, "MWSPersistenStoreUploadOperation: Trying to start again on an executing operation");
        
        if self.isCancelled {
            return
        }
        
        self.requestCount += 1
        
        _uploaded = false
        self.uploading = true
    
//        request.send { (result, code, data) in
//            self.parseData(result:result, code: code, data: data)
//
//            self.uploading = false
//            self.uploaded = true
//        }
        
        do {
            try request.execute()
            //self.responseResult = request.resultItems
            try parseData(result: true, code: 200, data: request.resultItems, error: nil )
        } catch {
            NSLog( error.localizedDescription )
            try? parseData(result: false, code: -1, data: nil, error: error )
        }
        
        self.uploading = false
        self.uploaded = true
    }
    
    override var isAsynchronous: Bool {
        return true
    }
    
    override var isExecuting: Bool{
        return self.uploading
    }
    
    override var isFinished: Bool{
        return self.uploaded || self.isCancelled
    }
    
    func parseData(result:Bool, code:Int, data:Any?, error: Error?) throws {
        //let response = (self.store.delegate?.webStore(store: self.webStore, requestDidFinishWithResult:result, code: code, data: data))!
        let response = MPSRequestResponse(result: result, items: data, timestamp: TimeInterval())

        self.responseCode = code
        self.responseData = data
        self.responseError = error
        self.responseResult = response.result

        //self.store.didParseDataInOperation(self, result: data)
        
        try responseDidReceive(response: response)
    }

    // Function to override
    func responseDidReceive(response:MPSRequestResponse) throws {}
    
    // MARK - Parser methods
    
    func updateObjects(items:[Any], for entity:NSEntityDescription, relationships:[String]?) throws -> ([NSManagedObjectID], [NSManagedObjectID], [NSManagedObjectID]) {
        
        let objects = NSMutableSet()
        let insertedObjects = NSMutableSet()
        let updatedObjects = NSMutableSet()
        let relationshipNodes = NSMutableDictionary()
        relationShipsNodes(relationships: relationships, nodes: relationshipNodes)
        
        for i in items {
            let values = i as! [String : Any]
            try updateObject(values:values, fetchEntity:entity, objectID:nil, relationshipNodes: relationshipNodes, objectIDs:objects, insertedObjectIDs:insertedObjects, updatedObjectIDs:updatedObjects)
        }
        
        return (objects.allObjects as! [NSManagedObjectID], insertedObjects.allObjects as! [NSManagedObjectID], updatedObjects.allObjects as! [NSManagedObjectID])
    }

    func updateObject(values:[String:Any], fetchEntity:NSEntityDescription, objectID:NSManagedObjectID?, relationshipNodes:NSMutableDictionary?, objectIDs:NSMutableSet, insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) throws {
        
        var entity = fetchEntity
        var entityValues: [String:Any] = values
        let entityName = values["classname"] as! String // ?? fetchEntity.name!
        if entityName != fetchEntity.name {
            entity = fetchEntity.managedObjectModel.entitiesByName[entityName]!

            guard let identifierString = store.identifierForItem(values, entityName: entityName) else {
                throw MIOPersistentStoreError.identifierIsNull
            }
            
            let fr =  NSFetchRequest<NSManagedObject>(entityName: entityName)
            fr.entity = entity
            let new_request = store.delegate!.store(store: store, fetchRequest: fr, serverID: identifierString)!
            
            try new_request.execute( )
            
            entityValues = new_request.resultItems!.first as! [String:Any]
        }
//        if fetchEntity.subentities.first == nil {
//            entityName = fetchEntity.name!
//        }
//        
        
//        
//        if entity.name != entityName {
//            let ctx = (webStore.delegate?.mainContextForWebStore(store: webStore))!
//            entity = NSEntityDescription.entity(forEntityName: entityName, in: ctx)!
//        }
        
        // Check the objects inside values
        let parsedValues = try checkRelationships(values:entityValues, entity: entity, relationshipNodes: relationshipNodes, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
        
        guard let identifierString = store.identifierForItem(parsedValues, entityName: fetchEntity.name!) else {
            throw MIOPersistentStoreError.identifierIsNull
        }
        
        let version = store.versionForItem(values, entityName: fetchEntity.name!)
        
        // Check if the server is deleting the object and ignoring
//        if store.cacheNode(deletingNodeAtServerID:serverID, entity:entity) == true {
//            return
//        }
                
        var node = store.cacheNode(withIdentifier: identifierString, entity: entity)
        if node == nil {
            NSLog("New version: " + entity.name! + " (\(version))");
            node = store.cacheNode(newNodeWithValues: parsedValues, identifier: identifierString, version: version, entity: entity, objectID: objectID)
            insertedObjectIDs.add(node!.objectID)
        }
        else if version > node!.version{
            NSLog("Update version: \(entity.name!) (\(node!.version) -> \(version))")
            store.cacheNode(updateNodeWithValues: parsedValues, identifier: identifierString, version: version, entity: entity)
            updatedObjectIDs.add(node!.objectID)
        }
        
        objectIDs.add(node!.objectID)
    }
    
    private func checkRelationships(values : [String : Any], entity:NSEntityDescription, relationshipNodes : NSMutableDictionary?, objectIDs:NSMutableSet, insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) throws -> [String : Any] {
        
        var parsedValues: [ String: Any ] = [ "classname": values[ "classname" ] as! String ]
        
        for key in entity.propertiesByName.keys {
            
            let prop = entity.propertiesByName[key]
            if prop is NSAttributeDescription {
                
                //let serverKey = (webStore.delegate?.webStore(store: webStore, serverAttributeName: key, forEntity: entity))!
                let serverKey = key
                
                //let newValue = webStore.delegate?.webStore(store: webStore, valueForAttribute: prop! as! NSAttributeDescription, serverValue:values[serverKey])
                let newValue = values[serverKey]
                
                let attr = prop as! NSAttributeDescription
                if attr.attributeType == .dateAttributeType {
                    if let date = newValue as? Date {
                        parsedValues[key] = date
                    } else {
                        if let dateString = newValue as? String {
                            parsedValues[key] = parse_date( dateString )
                        }
                    }
                } else if attr.attributeType == .UUIDAttributeType {
                    parsedValues[key] = newValue is String ? UUID(uuidString: newValue as! String ) : newValue  // (newValue as! UUID).uuidString.upperCased( )
                } else if attr.attributeType == .transformableAttributeType {
                    parsedValues[key] = newValue == nil ? nil
                                      : try JSONSerialization.jsonObject(with: (newValue as! String).data(using: .utf8)!, options: [ .fragmentsAllowed ])
                } else if attr.attributeType == .decimalAttributeType {
                    parsedValues[key] = MIOCoreDecimalValue( newValue, nil )
                } else {
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
                
                //let serverKey = (webStore.delegate?.webStore(store: webStore, serverRelationshipName: key, forEntity: entity))!
                let serverKey = key
                
                if relationshipNodes?[key] == nil {
                    parsedValues[key] = values[serverKey]
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
                        try updateObject(values: serverValues!, fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: value!, entityName: relEntity.destinationEntity!.name!)
                        guard let identifierString = store.identifierForItem(value as! [String:Any], entityName: relEntity.destinationEntity!.name!) else {
                            throw MIOPersistentStoreError.identifierIsNull
                        }
                        parsedValues[key] = identifierString
                    }
                }
                else {
                    
                    var array = [String]()
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    // let serverValues = (value as? [Any]) != nil ? value as!  [Any] : []
                    let serverValues = value as! [Any]
                    for relatedItem in serverValues {
                        try updateObject(values: relatedItem as! [String:Any], fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode!, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: relatedItem, entityName: relEntity.destinationEntity!.name!)
                        guard let identifierString = store.identifierForItem(relatedItem as! [String:Any], entityName: relEntity.destinationEntity!.name!) else {
                            throw MIOPersistentStoreError.identifierIsNull
                        }
                        array.append(identifierString)
                    }
                    
                    parsedValues[key] = array
                }
            }
        }
        
        //parsedValues["relationshipIDs"] = relationshipsIDs
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

}
