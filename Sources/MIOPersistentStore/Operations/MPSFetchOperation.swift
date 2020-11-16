//
//  MPSFetchOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

class MPSFetchOperation: MPSPersistentStoreOperation
{
    override func responseDidReceive(response:MPSRequestResponse) throws {
        if response.result == true {
            if let values = response.items as? [[String:Any]] {
                let relationships = self.relationshipKeyPathsForPrefetching;
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = try updateObjects(items: values, for: self.entity, relationships:relationships)
            }
        }
    }
    
}
