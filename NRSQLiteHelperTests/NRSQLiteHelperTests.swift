//
//  NRSQLiteHelperTests.swift
//  NRSQLiteHelperTests
//
//  Created by Nirbhay Agarwal on 26/04/18.
//  Copyright © 2018 NSRover. All rights reserved.
//

import XCTest
@testable import NRSQLiteHelper

class NRSQLiteHelperTests: XCTestCase {

    let kTableName = "TestTable"
    let kTableColumnID = "ID"
    let kTableColumnString = "String"
    let kTableColumnInt = "Int"

    var dbHelper:NRSQLiteHelper?

    override func setUp() {
        super.setUp()
        NRSQLiteHelper.destroy(dbFile: nil)
        dbHelper = NRSQLiteHelper(dbFile: nil)
    }
    
    func testMain() {
        //Create a table
        XCTAssert(createTable())

        let threadCount = 50
        let writeExpectation = expectation(description: "Waiting for all writes")
        var writeCount = 0

        //Test insertion
        for counter in 0..<threadCount {
            DispatchQueue.global().async {
                let query = """
                INSERT INTO \(self.kTableName) \
                (\(self.kTableColumnString), \(self.kTableColumnInt)) \
                VALUES (?, ?)
                """
                XCTAssertNotNil(self.dbHelper?.executeInsertQuery(query: query, parameters: ["A String Value", counter]))
                writeCount += 1
                if writeCount == 50 {
                    writeExpectation.fulfill()
                }
            }
        }

        wait(for: [writeExpectation], timeout: 5)

        //Test fetching
        let fetchQuery = """
        SELECT * FROM \(kTableName)
        """
        if let rows = dbHelper?.executeFetchQuery(query: fetchQuery, parameters: nil) {
            XCTAssert(rows.count == 50)
        } else {
            XCTAssert(false)
        }
    }

    //MARK: - Utilities

    func createTable() -> Bool {
        guard let dbHelper = dbHelper else {
            return false
        }

        let createTableQuery = """
        CREATE TABLE IF NOT EXISTS \(kTableName) \
        (\(kTableColumnID) INTEGER PRIMARY KEY AUTOINCREMENT, \
        \(kTableColumnString) TEXT, \
        \(kTableColumnInt) INTEGER)
        """

        return dbHelper.executeQuery(query: createTableQuery, parameters: nil)
    }
}
