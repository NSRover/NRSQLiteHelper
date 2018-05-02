//
//  NRDataPersister.swift
//  Since
//
//  Created by Nirbhay Agarwal on 07/03/18.
//  Copyright © 2018 NSRover. All rights reserved.
//

import Foundation
import SQLite3

class NRDataPersister: NSObject {

    enum QueryType {
        case simple
        case insert
        case fetch
    }

    let nilIntValue = -9999
    let nilStringValue = "<empty-string>"

    //DB constants
    let kTableNameActivity = "ACTIVITY"
    let kActivityId = "ID"
    let kActivityName = "NAME"
    let kActivityDescription = "DESCRIPTION"
    let kActivityPreferred_gap = "PREFERRED_GAP"
    let kActivityType = "TYPE"

    let kTableNameOccurance = "OCCURANCE"
    let kOccuranceActivityId = "ACTIVITY_ID"
    let kOccuranceDate = "DATE"

    var db:OpaquePointer? = nil
    typealias activityReturnType = (id:Int, name:String, description:String?, preferredGap:Int?, type:Int?)
    typealias activityWithOccurancesType = (activity:activityReturnType, occurances:[TimeInterval]?)

    // MARK: Occurance

    func insertOccurance(forActivityId activityId:Int, date:TimeInterval) -> Int? {
        let query = """
        INSERT INTO \(kTableNameOccurance) \
        (\(kOccuranceActivityId), \(kOccuranceDate)) \
        VALUES (?, ?)
        """

        let parameters:[Any] = [activityId, date]

        let (success, rowId) = runInsertQuery(query: query, withParameters: parameters)
        if !success {
            print("Cound not insert row")
        }
        return rowId
    }

    func occurances(forActivityId activityId:Int) -> [TimeInterval]? {
        let query = "SELECT \(kOccuranceDate) FROM \(kTableNameOccurance) WHERE \(kOccuranceActivityId) = \(activityId)"
        let (success, values) = runOccuranceFetchQuery(query: query)
        if (values != nil) &&
            success {
            return values
        }
        return nil
    }

    // MARK: Activity

    func insertActivity(name:String, description:String?, preferredGap:Int?, type:Int?) -> Int? {
        let query = """
        INSERT INTO \(kTableNameActivity) \
        (\(kActivityName), \(kActivityDescription), \(kActivityPreferred_gap), \(kActivityType)) \
        VALUES (?, ?, ?, ?)
        """

        let parameters:[Any] = [name,
                                description ?? nilStringValue,
                                preferredGap ?? nilIntValue,
                                type ?? nilIntValue]

        let (success, rowId) = runInsertQuery(query: query, withParameters: parameters)
        if !success {
            print("Cound not insert row")
        }
        return rowId
    }

    func activity(forId id:Int) -> activityReturnType? {
        let query = "SELECT * FROM \(kTableNameActivity) WHERE \(kActivityId) = \(id)"
        let (success, activityValues) = runActivityFetchQuery(query: query, withParameters: nil)
        if (activityValues != nil) &&
            success {
            return activityValues
        }
        return nil
    }

    func allActivities() -> [activityWithOccurancesType]? {
        let query = "SELECT \(kActivityId) FROM \(kTableNameActivity)"
        let (success, _, values) = runQuery(query: query, withParameters: nil, typeOfQuery: .fetch)
        if success && values != nil {
            if let values = values {
                var activitiesData:[activityWithOccurancesType] = []
                for value in values {
                    if let activityId = value[kActivityId] as? Int {
                        if let activityData = activity(forId: activityId) {
                            //Load occurances data
                            let occurancesData = occurances(forActivityId: activityId)
                            activitiesData.append((activityData, occurancesData))
                        }
                    }
                }
                return activitiesData
            }
        }
        return nil
    }

    func activityExistsForId(id:Int) -> Bool {
        let query = "SELECT COUNT(*) FROM \(kTableNameActivity) WHERE \(kActivityId) = \(id)"
        let (result, _, rows) = runQuery(query: query, withParameters: nil,
                                                 typeOfQuery: .fetch)

        if (rows != nil) &&
            result {
            let row = rows![0]
            if let value = row["COUNT(*)"] as? Int {
                return (value >= 1)
            }
        }
        return false
    }

    // MARK: Internals

    func runActivityFetchQuery(query:String, withParameters parameters:Array<Any>?) -> (success:Bool, activityParams:activityReturnType?) {
        let (result, _, activityRows) = runQuery(query: query, withParameters: parameters,
                                                   typeOfQuery: .fetch)
        if result && activityRows != nil {
            if let activityRows = activityRows {
                let activityRow = activityRows[0]
                if let name = activityRow[kActivityName] as? String,
                    let description = activityRow[kActivityDescription] as? String,
                    let preferredGap = activityRow[kActivityPreferred_gap] as? Int,
                    let type = activityRow[kActivityType] as? Int,
                    let id = activityRow[kActivityId] as? Int {

                    let actualDescription:String? = (description == nilStringValue) ? nil : description
                    let actualPreferredGap:Int? = (preferredGap == nilIntValue) ? nil : preferredGap
                    let actualType:Int? = (type == nilIntValue) ? nil : type
                    return (result, activityReturnType(id, name, actualDescription, actualPreferredGap, actualType))
                }
            }
        }
        return (result, nil)
    }

    func runOccuranceFetchQuery(query:String) -> (success:Bool, occurances:[TimeInterval]?) {
        let (result, _, rows) = runQuery(query: query, withParameters: nil,
                                                 typeOfQuery: .fetch)
        if result && rows != nil {
            if let rows = rows {
                var occurances:[TimeInterval] = []
                for row in rows {
                    if let time = row[kOccuranceDate] as? TimeInterval {
                        occurances.append(time)
                    }
                }
                return (result, occurances)
            }
        }
        return (result, nil)
    }

    // MARK: SQL

    func runQuery(query:String, withParameters parameters:Array<Any>?) -> Bool {
        let (result, _, _) = runQuery(query: query,
                                      withParameters: parameters,
                                      typeOfQuery: .simple)
        return result
    }

    func runInsertQuery(query:String, withParameters parameters:Array<Any>?) -> (success:Bool, rowId:Int?) {
        let (result, rowId, _) = runQuery(query: query,
                                          withParameters: parameters,
                                          typeOfQuery: .insert)
        return (result, rowId)
    }

    func runQuery(query:String,
                  withParameters parameters:Array<Any>?,
                  typeOfQuery queryType:QueryType) -> (success:Bool, rowId:Int?, returnedRows:[[String:Any]]?) {

        let errorReturnValue:(Bool, Int?, [[String:Any]]?) = (false, nil, nil)

        var statement:OpaquePointer?
        if sqlite3_prepare(db, query, -1, &statement, nil) != SQLITE_OK {
            print("Error preparing query")
            return (false, nil, nil)
        }

        //Bind parameters
        if let parameters = parameters {
            for ii in 0..<parameters.count {
                let parameter:Any = parameters[ii]
                if parameter is Int {
                    let parameterAsInt:Int = parameter as! Int
                    if sqlite3_bind_int(statement, Int32(ii + 1), Int32(parameterAsInt)) != SQLITE_OK {
                        print("Error binding Int \(parameter)")
                        return errorReturnValue
                    }
                } else if parameter is Double {
                    let parameterAsDouble:Double = parameter as! Double
                    if sqlite3_bind_double(statement, Int32(ii + 1), parameterAsDouble) != SQLITE_OK {
                        print("Error binding Double \(parameter)")
                        return errorReturnValue
                    }
                } else if parameter is String {
                    let parameterAsString:NSString = parameter as! NSString
                    if sqlite3_bind_text(statement, Int32(ii + 1), parameterAsString.utf8String, -1, nil) != SQLITE_OK {
                        print("Error binding String \(parameter)")
                        return errorReturnValue
                    }
                } else {
                    print("Error: Unsupported type!")
                    return errorReturnValue
                }
            }
        }

        var rowId:Int? = nil
        var rowsToReturn:[[String:Any]]? = nil
            switch queryType {
            case .insert:
                if sqlite3_step(statement) != SQLITE_DONE {
                    print("Error execution statement")
                    return errorReturnValue
                }
                rowId = Int(sqlite3_last_insert_rowid(db))
                if rowId == 0 {
                    rowId = nil
                    return errorReturnValue
                }
            case .fetch:
                var rows = [[String:Any]]()
                while sqlite3_step(statement) == SQLITE_ROW {
                    let totalColumns = sqlite3_column_count(statement)
                    var dict = [String:Any]()
                    for ii in 0..<totalColumns {
                        let columnNameAsChar = String.init(cString: sqlite3_column_name(statement, ii))
                        let columnType = sqlite3_column_type(statement, ii)
                        switch columnType {
                        case SQLITE_INTEGER:
                            let intValue = Int(sqlite3_column_int(statement, ii))
                            dict[columnNameAsChar] = intValue
                        case SQLITE_FLOAT:
                            let doubleValue = Double(sqlite3_column_double(statement, ii))
                            dict[columnNameAsChar] = doubleValue
                        case SQLITE_TEXT:
                            let textValue = String(cString: sqlite3_column_text(statement, ii))
                            dict[columnNameAsChar] = textValue
                        default:
                            break
                        }
                    }
                    rows.append(dict)
                }
                rowsToReturn = rows
            default:
                if sqlite3_step(statement) != SQLITE_DONE {
                    print("Error execution statement")
                    return errorReturnValue
                }
            }

        sqlite3_finalize(statement)
        return (true, rowId, rowsToReturn)
    }

    override init() {
        super.init()
        let fileURL = try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false).appendingPathComponent("Activity.sqlite")
        self.db = openDatabaseAtLocation(location: fileURL.path)
        createTablesOnDb()
    }

    func createTablesOnDb() {
        let createActivityTableQuery = """
        CREATE TABLE IF NOT EXISTS \(kTableNameActivity) \
        (\(kActivityId) INTEGER PRIMARY KEY AUTOINCREMENT, \
        \(kActivityName) TEXT, \
        \(kActivityDescription) TEXT, \
        \(kActivityPreferred_gap) INTEGER, \
        \(kActivityType) INTEGER)
        """
        let success = runQuery(query: createActivityTableQuery, withParameters: nil)
        if !success {
            return
        }

        let createOccuranceTableQuery = """
        CREATE TABLE IF NOT EXISTS \(kTableNameOccurance) \
        (\(kOccuranceActivityId) INTEGER, \
        \(kOccuranceDate) REAL)
        """
        let successOfOccurance = runQuery(query: createOccuranceTableQuery, withParameters: nil)
        if !successOfOccurance {
            return
        }
    }

    func openDatabaseAtLocation(location:String) -> OpaquePointer? {
        var db:OpaquePointer?
        if sqlite3_open(location, &db) != SQLITE_OK {
            print("Error opening database")
        }
        return db
    }



}
