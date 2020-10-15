//
//  CoreDataTests.swift
//  
//
//  Created by Javier Segura Perez on 04/10/2020.
//

import CoreData
import XCTest
import MIOPersistentStore

fileprivate func newEntity(_ name:String, properties:[NSPropertyDescription]) -> NSEntityDescription
{
    let entity = NSEntityDescription()
    entity.name = name
    
    entity.properties = properties
    
    return entity
}

fileprivate func newAttribute(_ name:String, type:NSAttributeType, defaultValue:Any?, optional:Bool) -> NSAttributeDescription
{
    let attr = NSAttributeDescription()
    attr.name = name
    attr.attributeType = type
    attr.defaultValue = defaultValue
    attr.isOptional = optional
    
    return attr
}

fileprivate func newRelationship(_ name:String, toMany:Bool) -> NSRelationshipDescription
{
    let rel = NSRelationshipDescription()
    rel.name = name
    rel.maxCount = toMany ? 0 : 1
    
    return rel
}

fileprivate func TestEntities() -> [NSEntityDescription]
{
    var entities:[NSEntityDescription] = []
    
    entities.append(newEntity("Document", properties: [
        newAttribute("identifier", type: .UUIDAttributeType, defaultValue: nil, optional: false),
        newAttribute("name", type: .stringAttributeType, defaultValue: nil, optional: false),
        newAttribute("date", type: .dateAttributeType, defaultValue: nil, optional: false)
        ]))
    
    return entities
}

fileprivate func TestManagedObjectModel() -> NSManagedObjectModel
{
    let model = NSManagedObjectModel()

    model.entities = TestEntities()
 
    return model
}

fileprivate func TestManagedObjectConext() -> NSManagedObjectContext
{
    NSPersistentStoreCoordinator.registerStoreClass(MIOPersistentStore.self, forStoreType: MIOPersistentStore.type)
    
    let url = URL(string: "dltest")
    let description = NSPersistentStoreDescription(url:url!)
    description.type = MIOPersistentStore.type

    let container = NSPersistentContainer(name: "TestDB", managedObjectModel:TestManagedObjectModel())
    container.persistentStoreDescriptions = [description]
    
    container.loadPersistentStores(completionHandler: { (storeDescription, error) in
        if let error = error as NSError? {
            NSLog(error.localizedDescription)
            fatalError("Unresolved error \(error), \(error.userInfo)")
        }
    })
    
    return container.viewContext
}


final class CoreDataTests: XCTestCase
{
    func testCreateObject() {
        let moc = TestManagedObjectConext()
        
        let doc = NSEntityDescription.insertNewObject(forEntityName: "Document", into: moc)
        
    }
}
