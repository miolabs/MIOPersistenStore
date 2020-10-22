//
//  MPInsertOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

class MPSInsertOperation: MPSPersistentStoreOperation
{
    override func responseDidReceive(response:MPSRequestResponse) throws {
        if response.result == true {
            NSLog("Inserted!")
            let values = response.items as! [[String : Any]]
            let version = self.store.versionForItem(values[0], entityName: entity.name!)
            if version > 1 {
                let relationships = self.relationshipKeyPathsForPrefetching;
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = try updateObjects(items: [values], for: self.entity, relationships:relationships)
            }
        }
    }
    
}
