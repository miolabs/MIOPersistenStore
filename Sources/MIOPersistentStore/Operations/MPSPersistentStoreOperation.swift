//
//  MWSPersistentStoreOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation
#if os(macOS) || os(Linux)
import MIOCoreData
#else
import CoreData
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
    
    var saveCount = 0
    var dependencyIDs:[String] = []
    
    var responseResult = false
    var responseCode:Int = 0
    var responseData:Any?
        
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
        _identifier = identifier ?? UUID().uuidString
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
        
        try? request.execute()
        //self.responseResult = request.resultItems
        parseData(result: true, code: 200, data: request.resultItems)
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
        return (self.uploaded
                && self.responseResult == true)
                || self.isCancelled
    }
    
    func parseData(result:Bool, code:Int, data:Any?) {
        //let response = (self.store.delegate?.webStore(store: self.webStore, requestDidFinishWithResult:result, code: code, data: data))!
        let response = MPSRequestResponse(result: true, items: data, timestamp: TimeInterval())

        self.responseCode = code
        self.responseData = data
        self.responseResult = response.result

        //self.store.didParseDataInOperation(self, result: data)
        
        responseDidReceive(response: response)
    }

    // Function to override
    func responseDidReceive(response:MPSRequestResponse){

    }
    
    // MARK - Parser methods
    
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
        
//        var entityName = (store.delegate?.webStore(store: webStore, serverEntityNameForItem:values, entityName:fetchEntity.name!))!
//        if fetchEntity.subentities.first == nil {
//            entityName = fetchEntity.name!
//        }
//        
//        var entity = fetchEntity;
//        
//        if entity.name != entityName {
//            let ctx = (webStore.delegate?.mainContextForWebStore(store: webStore))!
//            entity = NSEntityDescription.entity(forEntityName: entityName, in: ctx)!
//        }
        
        // Check the objects inside values
        let parsedValues = checkRelationships(values:values, entity: entity, relationshipNodes: relationshipNodes, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
        
        guard let identifier = store.identifierForItem(parsedValues, entityName: fetchEntity.name!) else {
            return
        }
        
        let version = store.versionForItem(values, entityName: fetchEntity.name!)
        
        // Check if the server is deleting the object and ignoring
//        if store.cacheNode(deletingNodeAtServerID:serverID, entity:entity) == true {
//            return
//        }
                
        var node = store.cacheNode(withIdentifier: identifier, entity: entity)
        if node == nil {
            NSLog("New version: " + entity.name! + " (\(version))");
            node = store.cacheNode(newNodeWithValues: parsedValues, identifier: identifier, version: version, entity: entity, objectID: objectID)
            insertedObjectIDs.add(node!.objectID)
        }
        else if version > node!.version{
            NSLog("Update version: \(entity.name!) (\(node!.version) -> \(version))")
            store.cacheNode(updateNodeWithValues: parsedValues, identifier: identifier, version: version, entity: entity)
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
                
                //let serverKey = (webStore.delegate?.webStore(store: webStore, serverAttributeName: key, forEntity: entity))!
                let serverKey = key
                
                //let newValue = webStore.delegate?.webStore(store: webStore, valueForAttribute: prop! as! NSAttributeDescription, serverValue:values[serverKey])
                let newValue = values[serverKey]
                
                let attr = prop as! NSAttributeDescription
                if (attr.attributeType != .dateAttributeType) {
                    
                    if newValue != nil
                    && newValue is NSNull == false {
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
                else {
                    let date = newValue as? Date
                    parsedValues[key] = date
                }
            }
            else if prop is NSRelationshipDescription {
                
                //let serverKey = (webStore.delegate?.webStore(store: webStore, serverRelationshipName: key, forEntity: entity))!
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
                        updateObject(values: serverValues!, fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: value!, entityName: relEntity.destinationEntity!.name!)
                        let serverID = store.identifierForItem(value as! [String:Any], entityName: relEntity.destinationEntity!.name!)
                        relationshipsIDs[key] = serverID
                    }
                }
                else {
                    
                    var array = [String]()
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    // let serverValues = (value as? [Any]) != nil ? value as!  [Any] : []
                    let serverValues = value as! [Any]
                    for relatedItem in serverValues {
                        updateObject(values: relatedItem as! [String:Any], fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode!, objectIDs: objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: relatedItem, entityName: relEntity.destinationEntity!.name!)
                        let serverID = store.identifierForItem(value as! [String:Any], entityName: relEntity.destinationEntity!.name!)
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

}
