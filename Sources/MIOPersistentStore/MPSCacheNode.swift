//
//  MPSCacheNode.swift
//  
//
//  Created by Javier Segura Perez on 05/10/2020.
//

import Foundation

#if APPLE_CORE_DATA
import CoreData
#else
import MIOCoreData
#endif


open class MPSCacheNode : NSObject
{
    static func referenceID(withIdentifier identifier:String, entity:NSEntityDescription) -> String {
        return entity.name! + "://" + identifier
    }
    
    var _identifier:String
    var _entity:NSEntityDescription
    var _values:[String:Any]
    var _version:UInt64 = 0
    var _objectID:NSManagedObjectID
 
    open var version: UInt64 { return _version }
    open var referenceID:String { get { return _entity.name! + "://" + _identifier } }
    open var objectID:NSManagedObjectID { get { return _objectID } }
    
    init(identifier:String, entity:NSEntityDescription, withValues values:[String:Any], version:UInt64, objectID:NSManagedObjectID){
        _identifier = identifier
        _values = values
        _version = version
        _entity = entity
        _objectID = objectID
    }
    
    func update(withValues values:[String:Any], version: UInt64) {
        _values.merge(values, uniquingKeysWith: { (_, new) in new } )
        _node = nil
        _attributeValues = nil
        _version = version
    }
    
    var _node:NSIncrementalStoreNode?
    func storeNode() throws -> NSIncrementalStoreNode {
        if _node != nil { return _node! }
        _node = NSIncrementalStoreNode(objectID: _objectID, withValues: attributeValues(), version: _version)
        return _node!
    }
        
    var _attributeValues:[String:Any]?
    func attributeValues() -> [String:Any] {
        if _attributeValues != nil { return _attributeValues! }
        
        _attributeValues = [:]
        for (key, _) in _entity.attributesByName {
            if let value = _values[key] {
                _attributeValues![key] = value
            }
        }
        
        return _attributeValues!
    }
    
    func value(forRelationship relationship: NSRelationshipDescription) throws -> Any? {
        return _values[relationship.name]
    }
}
