//
//  MPSFetchRequest.swift
//  
//
//  Created by Javier Segura Perez on 18/7/22.
//

import MIOCore
import MIOCoreData

open class MPSFetchRequest : MPSRequest
{    
    open var entityName:String
    open var entityID:String?
    open var entity:NSEntityDescription
    open var predicate:NSPredicate?
    open var sortDescriptors: [NSSortDescriptor]?
    open var limit: Int32?
    open var offset: Int32?
    open var includeRelationships: [String]?
    open var version: Int64?
    
    open var changeValues: [String:Any?]?
    
    public init( entity:NSEntityDescription ){
        self.entity = entity
        self.entityName = entity.name!
        super.init()
    }
            
    public init( fetchRequest:NSFetchRequest<NSManagedObject> )
    {
        entity = fetchRequest.entity!
        entityName = entity.name!
        predicate = fetchRequest.predicate
        sortDescriptors = fetchRequest.sortDescriptors
        limit  = MIOCoreInt32Value( fetchRequest.fetchLimit  )
        offset = MIOCoreInt32Value( fetchRequest.fetchOffset )
        includeRelationships = fetchRequest.relationshipKeyPathsForPrefetching
        #if !APPLE_CORE_DATA
        version = fetchRequest.version
        #endif
        super.init()
    }
}
