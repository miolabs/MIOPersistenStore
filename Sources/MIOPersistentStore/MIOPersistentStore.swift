//
//  MIOPersistentStore.swift
//  MIOWebServices
//
//  Created by GodShadow on 26/11/2017.
//  Copyright © 2017 MIO Research Labs. All rights reserved.
//

import Foundation

import MIOCoreData

public protocol MIOPersistentStoreDelegate : NSObjectProtocol
{
    //func store(_ store:MWSPersistentStore, requestForEntityName entityName:String, url:URL, bodyData:Data?, httpMethod:String) -> MWSRequest
    
    func store(store:MIOPersistentStore, fetchRequest:NSFetchRequest<NSManagedObject>, serverID:String?) -> MPSRequest?
    
    //    func webStore(store: MWSPersistentStore, insertRequestForObject object: NSManagedObject, dependencyIDs:NSMutableArray) -> MWSRequest?
    //    func webStore(store: MWSPersistentStore, updateRequestForObject object: NSManagedObject, dependencyIDs: NSMutableArray) -> MWSRequest?
    //    func webStore(store: MWSPersistentStore, deleteRequestForObject object: NSManagedObject) -> MWSRequest?
    //
    //    func webStore(store: MWSPersistentStore, requestDidFinishWithResult result:Bool, code:Int, data:Any?) -> MWSRequestResponse
    //
    //    func webStore(store: MWSPersistentStore, serverIDForObject object:NSManagedObject) -> String
    
    func store(store: MIOPersistentStore, entityNameFromItem item:[String:Any], fetchEntityName: String) -> String
    func store(store: MIOPersistentStore, identifierFromItem item:[String:Any], fetchEntityName: String) -> String
    func store(store: MIOPersistentStore, versionFromItem item:[String:Any], fetchEntityName: String) -> UInt64
    
    func store(store: MIOPersistentStore, tableKeyForAttributeName attributeName: String, forEntity entity: NSEntityDescription) -> String
    func store(store: MIOPersistentStore, tableValueForAttribute attribute: NSAttributeDescription, value:Any?) -> Any?
    
    func store(store: MIOPersistentStore, tableKeyForRelationshipName relationshipName: String, forEntity entity: NSEntityDescription) -> String
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

public enum MIOPersistentStoreError :Error
{
    case NoStoreURL
    case InvalidRequest
    case ServerIDIsNull
}

open class MIOPersistentStore: NSIncrementalStore
{
    public static var type: String { return "MIOPersistentStore.MIOPersistentStore" }
    
    public override var type: String { return "MIOPersistentStore.MIOPersistentStore" }
    
    public var delegate: MIOPersistentStoreDelegate?        
    
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
    
    //    public override init(persistentStoreCoordinator root: NSPersistentStoreCoordinator?, configurationName name: String?, at url: URL, options: [AnyHashable : Any]? = nil) {
    //
    //        super.init(persistentStoreCoordinator: root, configurationName: name, at: url, options: options)
    //    }
    
    public override func loadMetadata() throws {
        
        guard let storeURL = url else {
            throw MIOPersistentStoreError.NoStoreURL
        }
        
        self.storeURL = storeURL
        let uuid = UUID.init()
        let metadata = [NSStoreUUIDKey: uuid.uuidString, NSStoreTypeKey: type]
        self.metadata = metadata
    }
    
    public override func execute(_ request: NSPersistentStoreRequest, with context: NSManagedObjectContext?) throws -> Any {
        
        switch request {
            
        case let fetchRequest as NSFetchRequest<NSManagedObject>:
            let obs = try fetchObjects(fetchRequest: fetchRequest, with: context!)
            return obs
            
        case let saveRequest as NSSaveChangesRequest:
            saveObjects(request: saveRequest, with: context!)
            return []
            
        default:
            throw MIOPersistentStoreError.InvalidRequest
        }
    }
    
    // MARK: - NSIncrementalStore override
    
    public override func newValuesForObject(with objectID: NSManagedObjectID, with context: NSManagedObjectContext) throws -> NSIncrementalStoreNode {
        
        let serverID = referenceObject(for: objectID) as! String
        
        // if (serverID == null) error ("Server ID is null.")
        
        let node = cacheNode(WithServerID: serverID, entity: objectID.entity)!
        if (node.version == 0) {
            fetchObject(With:serverID, entityName: objectID.entity.name!, context:context)
        }
        
        return node
    }
    
    public override func newValue(forRelationship relationship: NSRelationshipDescription, forObjectWith objectID: NSManagedObjectID, with context: NSManagedObjectContext?) throws -> Any {
        
        let serverID = referenceObject(for: objectID) as! String
        let referenceID = objectID.entity.name! + "://" + serverID;
        let relations = relationshipValuesByReferenceID[referenceID] as? NSMutableDictionary
        
        //if (referenceID == null) throw new Error("MWSPersistentStore: Asking objectID without referenceID");
        
        //let node = (cacheNode(WithServerID:serverID, entity: objectID.entity))!
        //let relations = node.value(forKey: "relationshipIDs") as? NSMutableDictionary
        
        if (relationship.isToMany == false) {
            guard let relRefID = relations?[relationship.name] as? String else {
                return NSNull()
            }
            
            var relNode = cacheNode(WithServerID: relRefID, entity: relationship.destinationEntity!)
            if relNode == nil {
                relNode = cacheNode(newNodeWithValues: [String : Any](), atServerID: relRefID, version: 0, entity: relationship.destinationEntity!, objectID: nil)
            }
            
            return relNode!.objectID
        }
        else {
            guard let relRefIDs = relations?[relationship.name] as? [String] else {
                return NSArray()
            }
            
            let array = NSMutableArray()
            for relRefID in relRefIDs {
                var relNode = cacheNode(WithServerID: relRefID, entity: relationship.destinationEntity!)
                if relNode == nil {
                    relNode = cacheNode(newNodeWithValues: [String : Any](), atServerID: relRefID, version: 0, entity: relationship.destinationEntity!, objectID: nil)
                }
                array.add(relNode!.objectID)
            }
            
            return array
        }
    }
    
    public override func obtainPermanentIDs(for array: [NSManagedObject]) throws -> [NSManagedObjectID] {
        
        var newArray = [NSManagedObjectID]()
        
        //        for obj in array {
        //            let serverID = (delegate?.webStore(store: self, serverIDForObject: obj))!
        ////            if serverID == "" {
        ////                print("Empty serverID")
        ////            }
        //            let objID = newObjectID(for: obj.entity, referenceObject: serverID)
        //            newArray.append(objID)
        //        }
        
        return newArray
    }
    
    public override func managedObjectContextDidRegisterObjects(with objectIDs: [NSManagedObjectID]) {
        
    }
    
    public override func managedObjectContextDidUnregisterObjects(with objectIDs: [NSManagedObjectID]) {
        
    }
    
    // MARK: - Cache Nodes in memory
    var objectsByEntityName = NSMutableDictionary()
    var nodesByReferenceID = [String:NSIncrementalStoreNode]()
    var relationshipValuesByReferenceID = NSMutableDictionary()
    
    let bundleIdentfier = Bundle.main.bundleIdentifier
    let cacheNodeQueue = DispatchQueue(label: "\(String(describing: Bundle.main.bundleIdentifier)).mws.cache-queue")
    
    func cacheNode(WithServerID serverID:String, entity:NSEntityDescription) -> NSIncrementalStoreNode? {
        
        let referenceID = entity.name! + "://" + serverID
        var node:NSIncrementalStoreNode?
        cacheNodeQueue.sync {
            node = nodesByReferenceID[referenceID];
        }
        return node
    }
    
    func cacheNode(newNodeWithValues values:[String:Any], atServerID serverID: String, version:UInt64, entity:NSEntityDescription, objectID:NSManagedObjectID?) -> NSIncrementalStoreNode {
        
        if (serverID == "") {
            print("Empty serverID")
        }
        
        let referenceID = entity.name! + "://" + serverID;
        let objID = objectID ?? newObjectID(for: entity, referenceObject: serverID)
        let node = NSIncrementalStoreNode(objectID: objID, withValues: values, version: version)
        let relationshipIDs = values["relationshipIDs"]
        
        cacheNodeQueue.sync {
            nodesByReferenceID[referenceID] = node;
            relationshipValuesByReferenceID[referenceID] = relationshipIDs
            //            if enableLog {
            //                NSLog("Inserting REFID: " + referenceID);
            //            }
        }
        
        if entity.superentity != nil {
            cacheParentNode(node: node, relationshipIDs: relationshipIDs, atServerID: serverID, entity: entity.superentity!)
        }
        
        return node;
    }
    
    func cacheParentNode(node: NSIncrementalStoreNode, relationshipIDs: Any?, atServerID serverID: String, entity:NSEntityDescription) {
        
        if (serverID == "") {
            print("Empty serverID")
        }
        
        let referenceID = entity.name! + "://" + serverID;
        
        cacheNodeQueue.sync {
            nodesByReferenceID[referenceID] = node;
            relationshipValuesByReferenceID[referenceID] = relationshipIDs
            //            if enableLog {
            //                NSLog("Inserting Parent REFID: " + referenceID);
            //            }
        }
        
        if entity.superentity != nil {
            cacheParentNode(node: node, relationshipIDs: relationshipIDs, atServerID: serverID, entity: entity.superentity!)
        }
    }
    
    func cacheNode(updateNodeWithValues values:[String:Any], atServerID serverID:String, version:UInt64, entity:NSEntityDescription) {
        
        let referenceID = entity.name! + "://" + serverID
        
        cacheNodeQueue.sync {
            let node = nodesByReferenceID[referenceID]
            node?.update(withValues: values, version: version)
            if let relationshipIDs = values["relationshipIDs"] {
                relationshipValuesByReferenceID[referenceID] = relationshipIDs
            }
            //self.updateRelationshipIDs(oldRelationshipIDs: relationshipValuesByReferenceID[referenceID], withNewRelationshipIDs: values["relationshipIDs"])
            //            if enableLog {
            //                NSLog("Updating REFID: " + referenceID)
            //            }
        }
    }
    
    func updateRelationshipIDs(oldRelationshipIDs : Any?, withNewRelationshipIDs newRelationshipIDs : Any?) {
        
        //        guard let oldRelations = oldRelationshipIDs as? NSMutableDictionary else {
        //            assertionFailure("oldRelations must exist");
        //            return;
        //        }
        //
        //        guard let newRelations = newRelationshipIDs as? NSMutableDictionary else {
        //            assertionFailure("newRelations must exist");
        //            return;
        //        }
        //
        //        var keysToDelete = [String]()
        //
        //        for key in oldRelations {
        //            if newRelations[key] != nil {
        //                // replace
        //                oldRelations[key] = newRelations[key]
        //            }
        //            else {
        //                // delete
        //                keysToDelete.append(key)
        //            }
        //        }
        //
        //        for key in newRelations {
        //            if oldRelations[key] == nil {
        //                // add
        //                oldRelations[key] = newRelations[key];
        //            }
        //        }
        //
        //        oldRelations.removeObjects(forKeys: keysToDelete)
    }
    
    func cacheNode(deleteNodeAtServerID serverID:String, entity:NSEntityDescription) {
        
        let referenceID = entity.name! + "://" + serverID
        cacheNodeQueue.sync {
            nodesByReferenceID.removeValue(forKey: referenceID)
            //            relationshipValuesByReferenceID.removeObject(forKey: referenceID)
            //            if enableLog {
            //                NSLog("Delete REFID: " + referenceID)
            //            }
        }
        
        if entity.superentity != nil {
            cacheNode(deleteNodeAtServerID: serverID, entity: entity.superentity!)
        }
    }
    
    func cacheNode(deletingNodeAtServerID serverID:String, entity:NSEntityDescription) -> Bool {
        
        var deleting = false
        let referenceID = entity.name! + "://" + serverID
        cacheNodeQueue.sync {
            deleting = deletedObjects.contains(referenceID)
        }
        
        if deleting == true {
            return true
        }
        
        if entity.superentity != nil {
            deleting = cacheNode(deletingNodeAtServerID: serverID, entity: entity.superentity!)
        }
        
        return deleting
    }
    
    
    let operationQueue = OperationQueue()
    
    // MARK: - Fetching objects from server and cache
    
    var fetchingObjects = [String : Bool]()
    func fetchObject(With serverID:String, entityName:String, context:NSManagedObjectContext){
        
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
        
        if let request = delegate?.store(store: self, fetchRequest: fetchRequest, serverID: nil) {
            
            if request.type == .Synchronous {
                return try executeFetchRequest_sync(request, context: context)
            }
            
            
            
            //
            //            let op = MWSFetchOperation(webStore:self, request:request, entity:fetchRequest.entity!, relationshipKeyPathsForPrefetching:fetchRequest.relationshipKeyPathsForPrefetching)
            //            op.completionBlock = {
            //
            //                var ctx = self.contextByRequest[fetchRequest]
            //                if ctx == nil {
            //                    ctx = context
            //                }
            //                else {
            //                    self.contextByRequest.removeValue(forKey: fetchRequest)
            //                }
            //
            //                ctx!.performAndWait {
            //                    for objID in op.insertedObjectIDs {
            //                        self.cacheObjectForContext(objID: objID, entity:objID.entity, context: ctx!, refresh: true)
            //                    }
            //
            //                    for objID in op.updatedObjectIDs {
            //                        self.cacheObjectForContext(objID: objID, entity:objID.entity, context: ctx!, refresh: true)
            //                    }
            //                }
            //
            //                if completion != nil {
            //                    completion!(op.objectIDs, op.insertedObjectIDs, op.updatedObjectIDs)
            //                }
            //            }
            //
            //            operationQueue.addOperation(op)
        }
        //
        //        guard let set = objectsByEntityName[fetchRequest.entityName!] as? NSMutableSet else {
        //            return []
        //        }
        //
        //        let realObjectsSet = NSMutableSet()
        //        for objID in set {
        //            let obj = context.object(with: objID as! NSManagedObjectID)
        //            realObjectsSet.add(obj)
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
        
        return []
    }
    
    func executeFetchRequest_sync(_ request:MPSRequest, context:NSManagedObjectContext) throws -> [Any] {
                
        try request.execute()
        
        let (objectIDs, _, _) = updateObjects(items: request.fetchedItems!, for: request.entity, relationships: nil)
        
        var objects:[NSManagedObject] = []
        for objID in objectIDs {
            self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
            let obj = try context.existingObject(with: objID)
            objects.append(obj)
        }
        
        return objects
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
                let serverID = referenceObject(for: objID) as! String
                let node = cacheNode(WithServerID: serverID, entity: superentity)
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
    let saveOperationQueue = OperationQueue()
    func saveObjects(request:NSSaveChangesRequest, with context:NSManagedObjectContext) {
        
        request.insertedObjects?.forEach({ (obj) in
            updateRelationShipsCaches(object: obj)
            insertObjectIntoServer(object: obj, context: context)
            //insertObjectIntoCache(object: obj)
        })
        
        request.updatedObjects?.forEach({ (obj) in
            updateRelationShipsCaches(object: obj)
            updateObjectOnServer(object: obj, context: context)
            //updateObjectOnCache(object: obj)
        })
        
        request.deletedObjects?.forEach({ (obj) in
            deleteObjectOnServer(object: obj, context: context)
            //deleteObjectOnCache(object: obj)
            //            let serverID = referenceObject(for: obj.objectID) as! String
            //            let referenceID = obj.entity.name! + "://" + serverID;
            //            relationshipValuesByReferenceID.removeObject(forKey: referenceID)
        })
        
        //uploadToServer();
        saveCount += 1;
    }
    
    func updateRelationShipsCaches(object:NSManagedObject){
        //        let serverID = referenceObject(for: object.objectID) as! String
        //        let referenceID = object.entity.name! + "://" + serverID;
        //        var relations:NSMutableDictionary?
        //        cacheNodeQueue.sync {
        //            relations = relationshipValuesByReferenceID[referenceID] as? NSMutableDictionary
        //        }
        //        if relations == nil {
        //            relations = NSMutableDictionary()
        //        }
        //        for prop in object.entity.properties {
        //            if prop is NSRelationshipDescription {
        //                let rel = prop as! NSRelationshipDescription
        //                if object.hasFault(forRelationshipNamed: rel.name) {continue}
        //                if rel.isToMany == false {
        //                    let obj = object.value(forKey: rel.name) as? NSManagedObject
        //                    if (obj == nil) {
        //                        relations?.removeObject(forKey: rel.name)
        //                    }
        //                    else {
        //                        let identifier = (delegate?.webStore(store: self, serverIDForObject: obj!))!
        //                        relations?.setValue(identifier, forKey: rel.name)
        //                    }
        //                }
        //                else {
        //                    let objs = object.value(forKey: rel.name) as? [NSManagedObject]
        //                    if objs == nil {
        //                        relations?.removeObject(forKey: rel.name)
        //                    }
        //                    else {
        //                        let array = NSMutableArray()
        //                        for o in objs! {
        //                            let identifier = (delegate?.webStore(store: self, serverIDForObject: o))!
        //                            array.add(identifier)
        //                        }
        //
        //                        relations?.setValue(array, forKey: rel.name)
        //                    }
        //                }
        //            }
        //        }
        //        relationshipValuesByReferenceID[referenceID] = relations
    }
    
    //    func filterOnlyAttributes(fromAllValuesOfManagedObject managedObject:NSManagedObject) -> [String:Any] {
    
    //        let attribNames = Array(managedObject.entity.attributesByName.keys)
    //        var valuesDict = [String:Any]()
    //        for key in attribNames {
    //            let value = managedObject.value(forKey: key)
    //            if (value is NSNull
    //                || value == nil) {
    //                continue
    //            }
    //
    //            valuesDict[key] = value
    //        }
    //        return valuesDict
    //    }
    
    func insertObjectIntoServer(object:NSManagedObject, context:NSManagedObjectContext){
        
        //        let serverID = (delegate?.webStore(store: self, serverIDForObject: object))!
        //
        //        let attribValues = self.filterOnlyAttributes(fromAllValuesOfManagedObject: object)
        //
        //        _ = cacheNode(newNodeWithValues: attribValues, atServerID: serverID, version: 1, entity: object.entity, objectID: object.objectID)
        //
        //        cacheObjectForContext(objID: object.objectID, entity:object.entity, context: context, refresh: false)
        //
        //        let dependencies = NSMutableArray()
        //        guard let request = delegate?.webStore(store: self, insertRequestForObject: object, dependencyIDs:dependencies) else {
        //            return
        //        }
        //
        //        let op = MWSInsertOperation(webStore:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil)
        //        op.webStoreCache = persistentStoreCache
        //        op.completionBlock = {
        //            context.performAndWait {
        //                for objID in op.insertedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh:true)
        //                }
        //
        //                for objID in op.updatedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
        //                }
        //            }
        //
        //            if op.responseCode != 200 {
        //                NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
        //            }
        //        }
        //
        //        op.serverID = serverID
        //        op.dependencyIDs = dependencies
        //        op.saveCount = saveCount
        //        addOperation(operation: op, serverID:serverID)
    }
    
    func updateObjectOnServer(object:NSManagedObject, context:NSManagedObjectContext) {
        
        //        let serverID = (delegate?.webStore(store: self, serverIDForObject: object))!
        //        let node = (cacheNode(WithServerID: serverID, entity: object.entity))!
        //
        //        let attribValues = self.filterOnlyAttributes(fromAllValuesOfManagedObject: object)
        //
        //        _ = cacheNode(updateNodeWithValues: attribValues, atServerID: serverID, version: node.version + 1, entity: object.entity)
        //
        //        let dependencies = NSMutableArray()
        //        guard let request = delegate?.webStore(store: self, updateRequestForObject: object, dependencyIDs: dependencies) else {
        //            return
        //        }
        //
        //        let op = MWSUpdateOperation(webStore:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil)
        //        op.webStoreCache = persistentStoreCache
        //        op.nodeVersion = node.version
        //        op.completionBlock = {
        //            context.performAndWait {
        //                for objID in op.insertedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
        //                }
        //
        //                for objID in op.updatedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:object.entity, context: context, refresh: true)
        //                }
        //            }
        //
        //            if op.responseCode != 200 {
        //                NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
        //            }
        //
        //        }
        //
        //        op.serverID = serverID
        //        op.dependencyIDs = dependencies
        //        op.saveCount = saveCount
        //        addOperation(operation: op, serverID:serverID)
    }
    
    let deletedObjects = NSMutableSet()
    func deleteObjectOnServer(object:NSManagedObject, context:NSManagedObjectContext) {
        
        //        let serverID = (delegate?.webStore(store: self, serverIDForObject: object))!
        //        cacheNode(deleteNodeAtServerID: serverID, entity: object.entity)
        //        let referenceID = object.entity.name! + "://" + serverID
        //        deletedObjects.add(referenceID)
        //        self.deleteCacheObjectForContext(objID: object.objectID, entity: object.entity, context: context)
        //
        //        guard let request = delegate?.webStore(store: self, deleteRequestForObject: object) else {
        //            return
        //        }
        //
        //        let op = MWSDeleteOperation(webStore:self, request:request, entity:object.entity, relationshipKeyPathsForPrefetching:nil)
        //        op.webStoreCache = persistentStoreCache
        //        op.completionBlock = {
        //            context.performAndWait {
        //                for objID in op.insertedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
        //                }
        //
        //                for objID in op.updatedObjectIDs {
        //                    self.cacheObjectForContext(objID: objID, entity:objID.entity, context: context, refresh: true)
        //                }
        //
        //            }
        //            if op.responseCode == 200 {
        //                //TODO PARCHACO BORRADOS
        //                /*self.cacheNodeQueue.async {
        //                    self.deletedObjects.remove(referenceID)
        //                }*/
        //            }
        //            else if op.responseCode != 200 {
        //                NotificationCenter.default.post(name: NSNotification.Name(rawValue: "MWSPersistentStoreDidUpdateError"), object: object.objectID, userInfo:op.responseData as? [AnyHashable : Any])
        //            }
        //        }
        //
        //        op.serverID = serverID
        //        addOperation(operation: op, serverID: serverID)
    }
    
    //    var saveOperationsByReferenceID = [String:MWSPersistentStoreOperation]()
    //    var uploadingOperations = [String:Any]()
    //
    //    func addOperation(operation:MWSPersistentStoreOperation, serverID:String){
    //        saveOperationsByReferenceID[serverID] = operation
    //    }
    //
    //    func removeOperation(operation:MWSPersistentStoreOperation, serverID:String){
    //        saveOperationsByReferenceID[serverID] = nil
    //    }
    //
    //    func operationAtServerID(serverID:String, saveCount:Int) -> MWSPersistentStoreOperation? {
    //        return saveOperationsByReferenceID[serverID]
    //    }
    //
    //    func checkOperationDependecies(operation: MWSPersistentStoreOperation, dependencies:NSArray) {
    //
    //        for referenceID in dependencies {
    //            var op = operationAtServerID(serverID:referenceID as! String, saveCount:saveCount)
    //            if (op == nil) {
    //                op = lastUploadingOperationByServerID(serverID:referenceID as! String);
    //            }
    //            if (op == nil) {
    //                continue;
    //            }
    //            operation.addDependency(op!)
    //        }
    //    }
    //
    //    func uploadToServer() {
    //
    //        // Read from cache to know if there's any pending operation
    //        if (persistentStoreCache != nil) {
    //            let operations = persistentStoreCache!.pendingOperations()
    //            for pendingOperation in operations {
    //                addUploadingOperation(operation: pendingOperation, serverID: pendingOperation.serverID!)
    //                saveOperationQueue.addOperation(pendingOperation);
    //            }
    //        }
    //
    //        for (refID, op) in saveOperationsByReferenceID {
    //            checkOperationDependecies(operation:op, dependencies: op.dependencyIDs);
    //            addUploadingOperation(operation:op, serverID:refID);
    //            saveOperationQueue.addOperation(op);
    //        }
    //
    //        saveOperationsByReferenceID = [String:MWSPersistentStoreOperation]()
    //    }
    //
    //    func addUploadingOperation(operation:MWSPersistentStoreOperation, serverID:String){
    //
    //        var array = uploadingOperations[serverID] as? NSMutableArray;
    //        if array == nil {
    //            array = NSMutableArray()
    //            uploadingOperations[serverID] = array;
    //        }
    //        else {
    //            let lastOP = array?.lastObject as! MWSPersistentStoreOperation;
    //            operation.addDependency(lastOP);
    //        }
    //
    //        array?.add(operation);
    //    }
    //
    //    func lastUploadingOperationByServerID(serverID:String) -> MWSPersistentStoreOperation? {
    //        let array = uploadingOperations[serverID] as? NSMutableArray;
    //        if (array == nil) {
    //            return nil;
    //        }
    //        if (array!.count == 0) {
    //            return nil;
    //        }
    //        return array!.lastObject as? MWSPersistentStoreOperation;
    //    }
    
    
    //
    //
    //
    
    
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
                
                guard let serverKey = delegate?.store(store: self, tableKeyForAttributeName: key, forEntity: entity) else { continue }
                
                let newValue = delegate?.store(store: self, tableValueForAttribute: prop! as! NSAttributeDescription, value:values[serverKey])
                
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
                
                guard let serverKey = delegate?.store(store: self, tableKeyForRelationshipName: key, forEntity: entity) else { continue }
                
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
                        let serverID = delegate?.store(store: self, identifierFromItem: value as! [String:Any], fetchEntityName: relEntity.destinationEntity!.name!)
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

