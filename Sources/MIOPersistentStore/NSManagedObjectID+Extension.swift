//
//  NSManagedObjectID+Extension.swift
//  MIOPersistentStore
//
//  Created by Javier Segura Perez on 16/1/25.
//

import MIOCoreData

extension NSManagedObjectID
{
    public var referenceID: String {        
        return uriRepresentation().lastPathComponent
    }
}
