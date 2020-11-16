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
            let values = response.items as! [[String : Any]]
            if values.count == 0 { return }
            NSLog("Inserted: \(entity.name!), id = \(values[0]["identifier"]!)")
            let relationships = self.relationshipKeyPathsForPrefetching;
            (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = try updateObjects(items: values, for: self.entity, relationships:relationships)
        }
    }
}
