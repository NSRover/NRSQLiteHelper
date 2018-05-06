# NRSQLiteHelper
A barebones, easy to integrate SQLite wrapper written in Swift. This class provides the basic functionality required
from a SQLite db. It also includes the ability to bind parameters to SQL statement.

For common requirements this class provides all the funcionality out of the box. But SQL requirements may vary from project 
to project, and the simplicity of this class should make it easy to modify the code as your app grows.


## Adding to your project
Drop the `NRSQLiteHelper.swift` file into your project and you are good to go. You can additionally add unit test file 
`NRSQLiteHelperTests.swift` to test any changes you make to the code.

## Usage

Start by initialising an instance of `NRSQLiteHelper`. You can optionally skip the database file name by leaving it empty in 
which case a default name of `Database.sqlite` is assumed.
```swift
let dbHelper = NRSQLiteHelper(dbFile: nil)
```
or
```swift
let dbHelper = NRSQLiteHelper(dbFile: "Activities.sqlite")
```
NRSQLiteHelper uses a failiable initialiser and will return nil if it fails to open a database at the desired location. You 
can use a guard statement to check for this.
```swift
guard let dbHelper = NRSQLiteHelper(dbFile: dbFileName) else {
            print("Something went wrong")
        }
```

NRSQLiteHelper provides database operations through three simple APIs
```swift
func executeQuery(query: String, parameters: [Any]?) -> Bool
func executeInsertQuery(query: String, parameters: [Any]?) -> Int?
func executeFetchQuery(query: String, parameters: [Any]?) -> [[String : Any]]?
```
