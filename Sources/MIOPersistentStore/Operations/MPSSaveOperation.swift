//
//  File.swift
//  
//
//  Created by David Trallero on 18/7/22.
//

import Foundation

class MPSSaveOperation: MPSPersistentStoreOperation
{
    override func responseDidReceive(response:MPSRequestResponse) throws {
        if response.result == true {
            let values = response.items as? [[String : Any]]
            if values != nil && values!.count > 0 {
                let relationships = self.relationshipKeyPathsForPrefetching
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = try updateObjects(items: values!, for: self.entity, relationships:relationships)
            }
        }
    }
    
}
