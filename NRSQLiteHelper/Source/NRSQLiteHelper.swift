//
//  NRSQLiteHelper.swift
//  Since
//
//  Created by Nirbhay Agarwal on 23/03/18.
//  Copyright © 2018 NSRover. All rights reserved.
//

import Foundation
import SQLite3

class NRSQLiteHelper {

    private let databaseName:String
    private var db:OpaquePointer?

    //Serialsation
    private let queue = DispatchQueue(label: "DBSerialiser")

    private enum QueryType {
        case simple
        case insert
        case fetch
    }

    private let SQLITE_TRANSIENT = unsafeBitCast(-1, to:sqlite3_destructor_type.self)

    // MARK: Initialisation

    init?(dbFile:String?) {
        databaseName = dbFile ?? "Database.sqlite"
        let fileURL = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false).appendingPathComponent(databaseName)

        //Open database
        if sqlite3_open(fileURL.path, &db) != SQLITE_OK {
            print("Error opening database")
            return nil
        }
        print("Database path: \(fileURL.path)")
    }

    class func destroy(dbFile:String?) {
        let databaseName = dbFile ?? "Database.sqlite"
        let fileURL = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false).appendingPathComponent(databaseName)
        if FileManager.default.fileExists(atPath: fileURL.path) {
            do {
                try FileManager.default.removeItem(at: fileURL)
            } catch {
                print("Exception while destroying database: \(error)")
            }
        }
    }

    // MARK: SQL

    func executeQuery(query:String, parameters:[Any]?) -> Bool {
        var returnValue = false
        queue.sync {
            if let statement = prepare(query: query, parameters: parameters) {
                let result = sqlite3_step(statement)
                if result == SQLITE_DONE {
                    returnValue = true
                } else {
                    print("Error \(result) when executing query \(query)")
                }
                sqlite3_finalize(statement)
            }
        }
        return returnValue
    }

    func executeInsertQuery(query:String, parameters:[Any]?) -> Int? {
        var returnValue:Int? = nil
        queue.sync {
            if let statement = prepare(query: query, parameters: parameters) {
                let result = sqlite3_step(statement)
                if result == SQLITE_DONE {
                    let rowId = Int(sqlite3_last_insert_rowid(db))
                    if rowId > 0 {
                        returnValue = rowId
                    } else {
                        print("Error fetching rowId when executing query \(query)")
                    }
                } else {
                    print("Error \(result) when executing query \(query)")
                }
                sqlite3_finalize(statement)
            }
        }
        return returnValue
    }

    func executeFetchQuery(query:String, parameters:[Any]?) -> [[String:Any]]? {
        var returnValue:[[String:Any]]? = nil
        queue.sync {
            if let statement = prepare(query: query, parameters: parameters) {
                var rows = [[String:Any]]()
                while sqlite3_step(statement) == SQLITE_ROW {
                    let totalColumns = sqlite3_column_count(statement)
                    var row = [String:Any]()
                    for ii in 0..<totalColumns {
                        let columnNameAsChar = String.init(cString: sqlite3_column_name(statement, ii))
                        let columnType = sqlite3_column_type(statement, ii)
                        switch columnType {
                        case SQLITE_INTEGER:
                            let intValue = Int(sqlite3_column_int(statement, ii))
                            row[columnNameAsChar] = intValue
                        case SQLITE_FLOAT:
                            let doubleValue = Double(sqlite3_column_double(statement, ii))
                            row[columnNameAsChar] = doubleValue
                        case SQLITE_TEXT:
                            let textValue = String(cString: sqlite3_column_text(statement, ii))
                            row[columnNameAsChar] = textValue
                        default:
                            break
                        }
                    }
                    rows.append(row)
                }
                returnValue = rows
            }
        }
        return returnValue
    }

    private func prepare(query:String, parameters:[Any]?) -> OpaquePointer? {
        guard let db = db else {
            print("Database not initialised before prepare")
            return nil
        }

        var statement:OpaquePointer? = nil
        guard let cQuery = query.cString(using: .utf8) else {
            print("Error converting \(query) to utf8")
            return nil
        }

        let result = sqlite3_prepare_v2(db, cQuery, -1, &statement, nil)
        if result != SQLITE_OK {
            sqlite3_finalize(statement)
            print("Error preparing statement \(query)")
            return nil
        }

        //Bind paramenters
        if let params = parameters {
            //Validate if number of parameters match the count in the query
            let queryCount = sqlite3_bind_parameter_count(statement)
            if queryCount != CInt(params.count) {
                print("Number of parameters passed do not match the number expected in Query")
                return nil
            }

            var bindResult:CInt = SQLITE_OK
            for ii in 1...params.count {
                let parameter:Any = params[ii - 1]
                if let paramInt = parameter as? Int {
                    bindResult = sqlite3_bind_int(statement, CInt(ii), CInt(paramInt))
                } else if let paramDouble = parameter as? Double {
                    bindResult = sqlite3_bind_double(statement, CInt(ii), CDouble(paramDouble))
                } else if let paramString = parameter as? String {
                    bindResult = sqlite3_bind_text(statement, CInt(ii), paramString, -1, SQLITE_TRANSIENT)
                } else {
                    bindResult = sqlite3_bind_null(statement, CInt(ii))
                }

                //Check binding result
                if bindResult != SQLITE_OK {
                    sqlite3_finalize(statement)
                    print("Error binding \(parameter) in sql prepare statement")
                    return nil
                }
            }
        }

        return statement
    }
}
