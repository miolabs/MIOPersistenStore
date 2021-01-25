//
//  CoreDataTests.swift
//  
//
//  Created by Javier Segura Perez on 04/10/2020.
//

#if APPLE_CORE_DATA

import XCTest
import CoreData

@testable import MIOPersistentStore_CoreData


fileprivate func TestManagedObjectModel() -> NSManagedObjectModel
{
    let path = CommandLine.arguments.count < 2 ? "\(FileManager().currentDirectoryPath)" : CommandLine.arguments[1]

    let url = URL(string: path)!.appendingPathComponent("/Resources/TestModel.momd")
    
    let model = NSManagedObjectModel(contentsOf: url)
 
    return model!
}


var persistentContainer:NSPersistentContainer?

fileprivate func TestManagedObjectConext() -> NSManagedObjectContext
{
    if persistentContainer != nil { return persistentContainer!.viewContext }
    
    NSPersistentStoreCoordinator.registerStoreClass(MIOPersistentStore.self, forStoreType: MIOPersistentStore.storeType)
    
    let url = URL(string: "dltest")
    let description = NSPersistentStoreDescription(url:url!)
    description.type = MIOPersistentStore.storeType

    let container = NSPersistentContainer(name: "TestDB", managedObjectModel:TestManagedObjectModel())
    container.persistentStoreDescriptions = [description]
    
    container.loadPersistentStores(completionHandler: { (storeDescription, error) in
        if let error = error as NSError? {
            NSLog(error.localizedDescription)
            fatalError("Unresolved error \(error), \(error.userInfo)")
        }
    })
    
    persistentContainer = container
    
    return container.viewContext
}

fileprivate func TestPersistentStore(delegate:MIOPersistentStoreDelegate) -> MIOPersistentStore
{
    let store = persistentContainer!.persistentStoreCoordinator.persistentStores[0] as! MIOPersistentStore
    store.delegate = delegate
    return store
}

var itemsByEntity:[String:Any] = [:]

class TestRequest : MPSRequest
{
    enum RequestType {
        case fetch
        case insert
        case update
        case delete
    }
    
    var requestType:RequestType
    
    override init(fetchRequest: NSFetchRequest<NSManagedObject>) {
        requestType = .fetch
        super.init(fetchRequest: fetchRequest)
    }

    init(object:NSManagedObject, requestTYpe:RequestType) {
        self.requestType = requestTYpe
        super.init(entity: object.entity)
    }
    
    override func execute() throws {
        
        switch requestType {
        case .fetch:
            fetchValues()
            
        case .insert:
            insertValues()
            
        default:
            break
        }
        
    }
    
    func fetchValues() {
        let array = itemsByEntity[entityName] as? [Any]
        
        resultItems = array ?? []
    }
    
    func insertValues(){
        if changeValues == nil { return }
        
        DispatchQueue.global(qos: .background).sync {
            var array = itemsByEntity[entityName] as? NSMutableArray
            
            if array == nil {
                array = NSMutableArray()
                itemsByEntity[entityName] = array!
            }
            
            array!.add(changeValues!)
            
            
            resultItems = Array(array!)
        }
    }
}


final class CoreDataTests: XCTestCase, MIOPersistentStoreDelegate
{
    //
    // MIO Persistent Store Delegate methods
    //
    
    func store(store: MIOPersistentStore, fetchRequest: NSFetchRequest<NSManagedObject>, serverID: String?) -> MPSRequest? {
        let request = TestRequest(fetchRequest:fetchRequest)
        return request
    }
    
    func store(store: MIOPersistentStore, insertRequestForObject object: NSManagedObject, dependencyIDs: inout [String]) -> MPSRequest? {
        let request = TestRequest(object: object, requestTYpe: .insert)
        request.changeValues = serializeValues(store, fromObject: object)
        return request
    }
    
    func store(store: MIOPersistentStore, updateRequestForObject object: NSManagedObject, dependencyIDs: inout [String]) -> MPSRequest? {
        let request = TestRequest(object: object, requestTYpe: .update)
        return request
    }
    
    func store(store: MIOPersistentStore, deleteRequestForObject object: NSManagedObject) -> MPSRequest? {
        let request = TestRequest(object: object, requestTYpe: .delete)
        return request
    }
    
    func store(store: MIOPersistentStore, identifierForObject object: NSManagedObject) -> UUID? {
        return object.value(forKey: "identifier") as? UUID
    }
    
    func store(store: MIOPersistentStore, identifierFromItem item: [String : Any], fetchEntityName: String) -> String? {
        return item["identifier"] as? String
    }
    
    func store(store: MIOPersistentStore, versionFromItem item: [String : Any], fetchEntityName: String) -> UInt64 {
        return item["version"] as? UInt64 ?? 1
    }
    
    func serializeValues(_ store:MIOPersistentStore, fromObject object: NSManagedObject, onlyChanges:Bool = true) -> [String:Any] {
        
        let objectValues = object.changedValues()
        
        var values:[String:Any] = [:]
        
        for p in object.entity.properties {

            if p is NSAttributeDescription {
                let attr = p as! NSAttributeDescription
                guard let value = objectValues[attr.name] else {
                    continue
                }
                
                values[attr.name] = parseValue(value: value, type: attr.attributeType)
            }
            else if p is NSRelationshipDescription {
                let rel = p as! NSRelationshipDescription
                
                if rel.isToMany == false {
                    guard let obj = objectValues[rel.name] as? NSManagedObject else {
                        continue
                    }

                    if let identifier = obj.value(forKey: "identifier") as? UUID {
                        values[rel.name] = identifier.uuidString.uppercased( )
                    }
                }
                else {
                    
                    guard let objects = objectValues[rel.name] as? Set<NSManagedObject> else {
                        continue
                    }
                    
                    var relationIDs:[String] = []
                    for obj in objects {
                        if let identifier = obj.value(forKey: "identifier") as? UUID {
                            relationIDs.append(identifier.uuidString.uppercased( ))
                        }
                    }
                    
                    values[rel.name] = relationIDs
                }
                
            }
        }
        
        return values
    }
    
    func parseValue(value:Any, type:NSAttributeType) -> Any {
 
        switch type
        {
            case .UUIDAttributeType:
                return (value as! UUID).uuidString.uppercased( )
            
            case .dateAttributeType:
                return ISO8601DateFormatter().string(from: value as! Date)
            
            default:
                return value
        }
    }
        
    //
    // Tests
    //
    
    
    func testCreateObject() {
        let moc = TestManagedObjectConext()
        _ = TestPersistentStore(delegate: self)
        
        let doc = NSEntityDescription.insertNewObject(forEntityName:"Document", into: moc) as! Document
        doc.identifier = UUID()
        doc.name = "001"
        
        try! moc.save()
        
        XCTAssertTrue(itemsByEntity.count == 1)
    }
    
    func testCreateObjectsWithRelationship() {
        let moc = TestManagedObjectConext()
        _ = TestPersistentStore(delegate: self)
        
        let doc = NSEntityDescription.insertNewObject(forEntityName:"Document", into: moc) as! Document
        doc.identifier = UUID()
        doc.name = "002"
        
        let line = NSEntityDescription.insertNewObject(forEntityName: "DocumentLine", into: moc) as! DocumentLine
        line.identifier = UUID()
        line.concept = "Test Concept"
        line.quantity = 10
        line.document = doc

        try! moc.save()
        
        XCTAssertTrue(itemsByEntity.count == 2)
        let documents = itemsByEntity["Document"] as? [Any]
        XCTAssertTrue(documents?.count == 2)
        XCTAssertTrue((itemsByEntity["DocumentLine"] as! [Any]).count == 1)
    }

    func testFetchObject() {
        let moc = TestManagedObjectConext()
        _ = TestPersistentStore(delegate: self)

        let request = NSFetchRequest<Document>(entityName: "Document")
        request.sortDescriptors = [NSSortDescriptor(key: "name", ascending: true)]

        let objects = try! moc.fetch(request)
        XCTAssertTrue(objects.count == 2)
        
        let doc1 = objects[0]
        XCTAssertTrue(doc1.name == "001")
        
        let doc2 = objects[1]
        XCTAssertTrue(doc2.name == "002")
        XCTAssertTrue(doc2.lines?.count == 1)
        
        let line = doc2.lines?.allObjects[0] as! DocumentLine
        XCTAssertTrue(line.concept == "Test Concept")
        
    }

    
}

#endif
