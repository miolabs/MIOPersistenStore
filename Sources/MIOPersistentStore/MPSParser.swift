//
//  File.swift
//  
//
//  Created by Javier Segura Perez on 28/8/23.
//

import Foundation
import MIOCoreData
import MIOCore

// MARK - Parser methods

extension MIOPersistentStore
{
    
    func updateObjects(items:[Any], for entity:NSEntityDescription, relationships:[String]?) throws -> ([NSManagedObjectID], [NSManagedObjectID], [NSManagedObjectID]) {
        
        var objects:[NSManagedObjectID] = []
        let insertedObjects = NSMutableSet()
        let updatedObjects = NSMutableSet()
        let relationshipNodes = NSMutableDictionary()
        relationShipsNodes(relationships: relationships, nodes: relationshipNodes)
        
        for i in items {
            let values = i as! [String : Any]
            try updateObject(values:values, fetchEntity:entity, objectID:nil, relationshipNodes: relationshipNodes, objectIDs:&objects, insertedObjectIDs:insertedObjects, updatedObjectIDs:updatedObjects)
        }
        
        return (objects, insertedObjects.allObjects as! [NSManagedObjectID], updatedObjects.allObjects as! [NSManagedObjectID])
    }

    func updateObject(values:[String:Any], fetchEntity:NSEntityDescription, objectID:NSManagedObjectID?, relationshipNodes:NSMutableDictionary?, objectIDs:inout [NSManagedObjectID], insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) throws {
        
        var entity = fetchEntity
        let entityValues: [String:Any] = values
        let entityName = values["classname"] as! String // ?? fetchEntity.name!
        if entityName != fetchEntity.name {
            entity = fetchEntity.managedObjectModel.entitiesByName[entityName]!
        }
            
        // Check the objects inside values
        let parsedValues = try checkRelationships( values:entityValues, entity: entity, relationshipNodes: relationshipNodes, objectIDs: &objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs )
        
        guard let identifier = identifierForItem( parsedValues, entityName: fetchEntity.name! ) else {
            throw MIOPersistentStoreError.identifierIsNull()
        }
        
        let version = versionForItem( values, entityName: fetchEntity.name! )
        
        // Check if the server is deleting the object and ignoring
        //        if store.cacheNode(deletingNodeAtServerID:serverID, entity:entity) == true {
        //            return
        //        }
        
        var node = try cacheNode( withIdentifier: identifier, entity: entity )
        if node == nil {
            // --- NSLog("New version: " + entity.name! + " (\(version))");
            node = try cacheNode( newNodeWithValues: parsedValues, identifier: identifier, version: version, entity: entity, objectID: objectID )
            insertedObjectIDs.add(node!.objectID)
        }
        else if version > node!.version {
            // --- NSLog("Update version: \(entity.name!) (\(node!.version) -> \(version))")
            try cacheNode( updateNodeWithValues: parsedValues, identifier: identifier, version: version, entity: entity )
            updatedObjectIDs.add( node!.objectID )
        }
        
        objectIDs.append( node!.objectID )
    }

    func checkRelationships(values : [String : Any], entity:NSEntityDescription, relationshipNodes : NSMutableDictionary?, objectIDs:inout [NSManagedObjectID], insertedObjectIDs:NSMutableSet, updatedObjectIDs:NSMutableSet) throws -> [String : Any] {
        
        var parsedValues: [ String: Any ] = [ "classname": values[ "classname" ] as! String ]
        
        for key in entity.propertiesByName.keys {
            
            let prop = entity.propertiesByName[key]
            if prop is NSAttributeDescription {
                                         
                // TODO: Transfrom key from UserInfo
                let serverKey = key
                
                let newValue = values[serverKey]
                if newValue == nil { continue }
                
                if newValue is NSNull {
                    parsedValues[key] = newValue
                    continue
                }
                                
                let attr = prop as! NSAttributeDescription
                if attr.attributeType == .dateAttributeType {
                    if let date = newValue as? Date {
                        parsedValues[key] = date
                    }
                    else if let dateString = newValue as? String {
                        parsedValues[key] = try parse_date( dateString )
                    }
                    else {
                        throw MIOPersistentStoreError.invalidValueType(entityName:entity.name!, key: key, value: newValue!)
                    }
                }
                else if attr.attributeType == .UUIDAttributeType {
                    parsedValues[key] = newValue is String ? UUID(uuidString: newValue as! String ) : newValue  // (newValue as! UUID).uuidString.upperCased( )
                }
                else if attr.attributeType == .transformableAttributeType {
                    parsedValues[key] = try JSONSerialization.jsonObject( with: ( newValue as! String ).data( using: .utf8 )!, options: [ .allowFragments ] )
                }
                else if attr.attributeType == .decimalAttributeType {
                    let decimal = MCDecimalValue( newValue, nil )
                    parsedValues[key] = decimal != nil ? decimal! : NSNull()
                }
                else {
                    // check type
                    switch attr.attributeType {
                    case .booleanAttributeType,
                         .decimalAttributeType,
                         .doubleAttributeType,
                         .floatAttributeType,
                         .integer16AttributeType,
                         .integer32AttributeType,
                         .integer64AttributeType:
                        if !(newValue is NSNumber) {
                            throw MIOPersistentStoreError.invalidValueType( entityName: entity.name!, key: key, value: newValue )
                        }
                        
                    case .stringAttributeType:
                        if !(newValue is NSString) {
                            throw MIOPersistentStoreError.invalidValueType( entityName: entity.name!, key: key, value: newValue )
                        }
                    
                    default:
                        throw MIOPersistentStoreError.invalidValueType( entityName: entity.name!, key: key, value: newValue )
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
                if value == nil { continue }
                
                if relEntity.isToMany == false {
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    if let serverValues = value as? [String:Any] {
                        try updateObject(values: serverValues, fetchEntity: relEntity.destinationEntity!, objectID: nil, relationshipNodes: relKeyPathNode, objectIDs: &objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: value!, entityName: relEntity.destinationEntity!.name!)
                        guard let identifierString = identifierForItem(value as! [String:Any], entityName: relEntity.destinationEntity!.name!) else {
                            throw MIOPersistentStoreError.identifierIsNull()
                        }
                        parsedValues[key] = identifierString
                    }
                }
                else {
                    
                    var array = [UUID]()
                    let relKeyPathNode = relationshipNodes![key] as? NSMutableDictionary
                    // let serverValues = (value as? [Any]) != nil ? value as!  [Any] : []
                    let serverValues = value as! [Any]
                    for relatedItem in serverValues {
                        
                        guard let ri = relatedItem as? [String:Any] else {
                            print("[MIOPersistentStoreOperation] item: \(relatedItem)")
                            throw MIOPersistentStoreError.invalidValueType( entityName: relEntity.name, key: serverKey, value: relatedItem )
                        }

                        guard let dst = relEntity.destinationEntity else {
                            print("[MIOPersistentStoreOperation] dst: \(String(describing: relEntity.destinationEntity))")
                            throw MIOPersistentStoreError.invalidValueType( entityName: relEntity.name, key: serverKey, value: relEntity.destinationEntity?.name ?? "relEntity.destinationEntity is nil" )
                        }

                        try updateObject(values: ri, fetchEntity: dst, objectID: nil, relationshipNodes: relKeyPathNode, objectIDs: &objectIDs, insertedObjectIDs: insertedObjectIDs, updatedObjectIDs: updatedObjectIDs)
                        //let serverID = webStore.delegate?.webStore(store: webStore, serverIDForItem: relatedItem, entityName: relEntity.destinationEntity!.name!)
                        guard let identifier = identifierForItem(relatedItem as! [String:Any], entityName: relEntity.destinationEntity!.name!) else {
                            throw MIOPersistentStoreError.identifierIsNull()
                        }
                        array.append( identifier )
                    }
                    
                    parsedValues[key] = array
                }
            }
        }
                
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
            }
        }
    }
}
