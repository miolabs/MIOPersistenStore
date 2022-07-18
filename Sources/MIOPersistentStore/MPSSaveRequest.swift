//
//  MPSSaveRequest.swift
//  
//
//  Created by Javier Segura Perez on 18/7/22.
//

import Foundation

#if APPLE_CORE_DATA
import CoreData
#else
import MIOCore
import MIOCoreData
#endif


open class MPSSaveRequest : MPSRequest
{        
    public init( saveRequest: NSSaveChangesRequest ) {
        super.init()
    }    
}
