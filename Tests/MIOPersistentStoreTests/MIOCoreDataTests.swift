//
//  MIOCoreDataTests.swift
//  
//
//  Created by Javier Segura Perez on 04/10/2020.
//

import XCTest
import MIOCoreData
import MIOPersistentStore

fileprivate func newEntity(_ name:String, properties:[MIOCoreData.NSPropertyDescription]) -> MIOCoreData.NSEntityDescription
{
    let entity = MIOCoreData.NSEntityDescription()
    entity.name = name
    
    entity.properties = properties
    
    return entity
}

fileprivate func newAttribute(_ name:String, type:MIOCoreData.NSAttributeType, defaultValue:Any?, optional:Bool) -> MIOCoreData.NSAttributeDescription
{
    let attr = MIOCoreData.NSAttributeDescription()
    attr.name = name
    attr.attributeType = type
    attr.defaultValue = defaultValue
    attr.isOptional = optional
    
    return attr
}

fileprivate func newRelationship(_ name:String, toMany:Bool) -> MIOCoreData.NSRelationshipDescription
{
    let rel = MIOCoreData.NSRelationshipDescription()
    rel.name = name
    rel.maxCount = toMany ? 0 : 1
    
    return rel
}

fileprivate func TestEntities() -> [MIOCoreData.NSEntityDescription]
{
    var entities:[MIOCoreData.NSEntityDescription] = []
    
    entities.append(newEntity("Document", properties: [
        newAttribute("identifier", type: .UUIDAttributeType, defaultValue: nil, optional: false),
        newAttribute("name", type: .stringAttributeType, defaultValue: nil, optional: false),
        newAttribute("date", type: .dateAttributeType, defaultValue: nil, optional: false)
        ]))
    
    return entities
}

fileprivate func TestManagedObjectModel() -> MIOCoreData.NSManagedObjectModel
{
    let model = MIOCoreData.NSManagedObjectModel()

    model.entities = TestEntities()
 
    return model
}

fileprivate func TestManagedObjectConext() -> MIOCoreData.NSManagedObjectContext
{
    MIOCoreData.NSPersistentStoreCoordinator.registerStoreClass(MIOPersistentStore.self, forStoreType: MIOPersistentStore.type)
    
    let url = URL(string: "dltest")
    let description = MIOCoreData.NSPersistentStoreDescription(url:url!)
    description.type = MIOPersistentStore.type

    let container = MIOCoreData.NSPersistentContainer(name: "TestDB", managedObjectModel:TestManagedObjectModel())
    container.persistentStoreDescriptions = [description]
    
    container.loadPersistentStores(completionHandler: { (storeDescription, error) in
        if let error = error as NSError? {
            NSLog(error.localizedDescription)
            fatalError("Unresolved error \(error), \(error.userInfo)")
        }
    })
    
    return container.viewContext
}


final class MIOCoreDataTests: XCTestCase
{
    func testCreateObject() {
        //let moc = TestManagedObjectConext()
        
        //let doc = NSEntityDescription.insertNewObject(forEntityName: "Document", into: moc)
    }
}
