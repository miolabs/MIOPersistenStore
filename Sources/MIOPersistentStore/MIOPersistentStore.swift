//
//  MIOPersistentStore.swift
//  MIOWebServices
//
//  Created by GodShadow on 26/11/2017.
//  Copyright © 2017 MIO Research Labs. All rights reserved.
//

import Foundation

//#if os(linux)
import MIOCoreData
//#else
//import CoreData

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
    public override var type: String {return "MIOPersistentStore"}
                
    var storeURL:URL?
    
//    public func fetchRequest(withEntityName entityName:String, context:NSManagedObjectContext) -> NSFetchRequest<NSFetchRequestResult>{
//        let request = NSFetchRequest<NSFetchRequestResult>()
//        let entity = NSEntityDescription.entity(forEntityName: entityName, in: context)
//        request.entity = entity
//
//        return request
//    }

//    var currentFetchContext:NSManagedObjectContext?
//    public func performFetchInFetchedResultsController(fetchResultsController:NSFetchedResultsController<NSFetchRequestResult>) throws {
//        currentFetchContext = fetchResultsController.managedObjectContext
//        try fetchResultsController.performFetch()
//        currentFetchContext = nil
//    }
    
    // MARK: - NSPersistentStore override
    
    public override init(persistentStoreCoordinator root: NSPersistentStoreCoordinator?, configurationName name: String?, at url: URL, options: [AnyHashable : Any]? = nil) {
        
        super.init(persistentStoreCoordinator: root, configurationName: name, at: url, options: options)
    }
    
    public override func loadMetadata() throws {
        
        guard let storeURL = url else {
            throw MIOPersistentStoreError.NoStoreURL
        }
        
        self.storeURL = storeURL
        let uuid = UUID.init()
        let metadata = [NSStoreUUIDKey: uuid.uuidString, NSStoreTypeKey: "MIOPersistentStore"]
        self.metadata = metadata
    }
        
    public override func execute(_ request: NSPersistentStoreRequest, with context: NSManagedObjectContext?) throws -> Any {
        
        switch request {
            
        case let fetchRequest as NSFetchRequest<NSManagedObject>:
            let obs = fetchObjects(fetchRequest: fetchRequest, with: context!,  completion: nil)
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
        
        var newArray = [NSManagedObjectID]();
        
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
    
    public typealias FetchObjectsCompletion = ([NSManagedObjectID], [NSManagedObjectID], [NSManagedObjectID]) -> Void
    public func fetchObjects(fetchRequest:NSFetchRequest<NSManagedObject>, with context:NSManagedObjectContext, completion:FetchObjectsCompletion?) -> [Any] {
    
        return []
        
//        if delegate == nil {
//            return [];
//        }
//
//        if fetchRequest.entity == nil {
//            fetchRequest.entity = NSEntityDescription.entity(forEntityName: fetchRequest.entityName!, in: context)
//        }
//
//        if currentFetchContext != nil {
//            contextByRequest[fetchRequest] = currentFetchContext
//        }
//
//        if let request = delegate?.webStore(store: self, fetchRequest: fetchRequest, serverID: nil) {
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
//        }
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
    }
    
    func  cacheObjectForContext(objID:NSManagedObjectID, entity:NSEntityDescription, context:NSManagedObjectContext, refresh:Bool){
        
//        do {
//            let obj = try context.existingObject(with: objID)
//            if refresh == true  {
//                context.refresh(obj, mergeChanges: true)
//            }
//            var set = self.objectsByEntityName[entity.name!] as? NSMutableSet
//            if set == nil {
//                set = NSMutableSet()
//            }
//            set?.add(obj.objectID)
//            self.objectsByEntityName[entity.name!] = set
//
//            if let superentity = entity.superentity {
//                let serverID = referenceObject(for: objID) as! String
//                let node = cacheNode(WithServerID: serverID, entity: superentity)
//                cacheObjectForContext(objID: node!.objectID, entity:superentity, context: context, refresh: refresh)
//            }
//        }
//        catch {
//            print("ERROR: \(error)")
//        }
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

    
}

