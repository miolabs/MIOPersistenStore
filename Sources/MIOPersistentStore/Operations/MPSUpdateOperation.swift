//
//  MWSUpdateOperation.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

class MPSUpdateOperation: MPSPersistentStoreOperation
{    
    public var nodeVersion = UInt64(1)
    
    override func responseDidReceive(response:MPSRequestResponse){
        if response.result == true {
            NSLog("Update!")
            let values = response.items as! [String : Any]
            let version = self.store.versionForItem(values, entityName: entity.name!)
            if version > nodeVersion {
                let relationships = self.relationshipKeyPathsForPrefetching;
                (self.objectIDs, self.insertedObjectIDs, self.updatedObjectIDs) = updateObjects(items: [values], for: self.entity, relationships:relationships)
            }
        }
    }
}
