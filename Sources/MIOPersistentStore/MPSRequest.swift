//
//  File.swift
//  
//
//  Created by Javier Segura Perez on 15/05/2020.
//

import Foundation

#if APPLE_CORE_DATA
import CoreData
#else
import MIOCore
import MIOCoreData
public typealias NSPredicate = MIOPredicate
#endif


//#else
//import MIOCoreData

//#endif

open class MPSRequest : NSObject
{    
    open var resultItems:[Any]?
    
    open var entityName:String
    open var entityID:String?
    open var entity:NSEntityDescription
    open var predicate:NSPredicate?
    open var sortDescriptors: [NSSortDescriptor]?
    open var limit: Int?
    open var offset: Int?
    open var includeRelationships: [String]?
    
    open var changeValues: [String:Any?]?
    
    
    public init(entity:NSEntityDescription){
        self.entity = entity
        self.entityName = entity.name!
        super.init()
    }
    
    public init(fetchRequest:NSFetchRequest<NSManagedObject>) {
        entity = fetchRequest.entity!
        entityName = entity.name!
        predicate = fetchRequest.predicate
        sortDescriptors = fetchRequest.sortDescriptors
        limit = fetchRequest.fetchLimit
        offset = fetchRequest.fetchOffset
        includeRelationships = fetchRequest.relationshipKeyPathsForPrefetching
        super.init()
    }
    
    open func execute() throws {}
}
