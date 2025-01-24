//
//  File.swift
//  
//
//  Created by Javier Segura Perez on 15/05/2020.
//

import Foundation
import MIOCore
import MIOCoreData


//#else
//import MIOCoreData

//#endif

open class MPSRequest
{    
    open var resultItems:[String:Any]?
    open func execute() throws {}
}
