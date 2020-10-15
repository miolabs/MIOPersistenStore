//
//  MPSDeleteOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

class MPSDeleteOperation: MPSPersistentStoreOperation
{
    override func responseDidReceive(response:MPSRequestResponse) {
        if response.result == true {
            NSLog("Delete!")
            let values = response.items as! [String : Any]
            if values.count > 0 {
                let relationships = self.relationshipKeyPathsForPrefetching
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = updateObjects(items: [values], for: self.entity, relationships:relationships)
            }
        }
    }
    
}
