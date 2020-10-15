//
//  MPSFetchOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

class MPSFetchOperation: MPSPersistentStoreOperation
{
    override func responseDidReceive(response:MPSRequestResponse){
        if response.result == true {
            if let values = response.items as? [Any] {
                let relationships = self.relationshipKeyPathsForPrefetching;
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = updateObjects(items: values, for: self.entity, relationships:relationships)
            }
        }
    }
    
}
