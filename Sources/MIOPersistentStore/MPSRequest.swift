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
#endif


//#else
//import MIOCoreData

//#endif

open class MPSRequest
{    
    open var resultItems:[Any]?
                        
    open func execute() throws {}
}
