//
//  MPSRequestResponse.swift
//  
//
//  Created by Javier Segura Perez on 24/09/2020.
//

import Foundation

public class MPSRequestResponse : NSObject {
    
    var result : Bool = false
    var items : Any?
    var timestamp : TimeInterval
    
    #if os(Linux)
    public init(result : Bool, items : Any?, timestamp : TimeInterval) {
        self.result = result
        self.items = items
        self.timestamp = timestamp
    }
    #else
    @objc
    public init(result : Bool, items : Any?, timestamp : TimeInterval) {
        self.result = result
        self.items = items
        self.timestamp = timestamp
    }
    #endif
        
    
}
