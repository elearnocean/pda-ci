using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dac;
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using Microsoft.SqlServer.Management.Smo;
using Microsoft.SqlServer.Management.Common;


namespace DacMini
{

    // The types of objects we support for reading and scripting out via the SQL server scripter 
    // (including a few special helper objects not used by the scripter, for other miscellaneous purposes)
    public enum SQLObjectType { ParseError, ScriptFile, Database, Table, View, StoredProcedure, PartitionScheme, PartitionFunction, Schema, FileGroup, User, Function }
    // Helper objects:
    // ParseError = An error - we won't try get this object.
    // ScriptFile = A file that contians a SQL script.
    // Database = A connection to a database and server.  Needed to extract all other SQL objects.

    public enum DacPacVersion { SQL2012, SQL2014, SQL2016, SQL2017 }
    // Different version of SQL DacPac that we can create.  We default to SQL2016 (for now);

    class SQLObjectName
    {
        public SQLObjectType ObjectType { get; set; }   // Type of object
        public string Schema { get; set; }              // Schema of SQL object (if it has one)
        public string Name { get; set; }                // Name of SQL object
        public string ScriptFile { get; set; }          // ScriptFile location (only used for ScriptFile object type)
        public string ServerName { get; set; }          // Server name for database connection.
        public string DatabaseName { get; set; }        // Database name for database connection.

        public SQLObjectName(string myObjectString)
        {
            if (myObjectString.Contains(':'))
            {
                // split object into pieces
                int cdelim = myObjectString.IndexOf(':');
                string myObjectType = myObjectString.Substring(0, cdelim).ToLower();
                string myObjectName = myObjectString.Substring(cdelim + 1, myObjectString.Length - cdelim - 1);
                string myObjectNameFirstPart = string.Empty;
                string myObjectNameSecondPart = string.Empty;

                if (myObjectName.Contains('.'))
                {
                    string[] myObjectParts = myObjectString.Substring(cdelim + 1, myObjectString.Length - cdelim - 1).Split('.');
                    myObjectNameFirstPart = myObjectParts[0];
                    myObjectNameSecondPart = myObjectParts[1];
                    // If there is more than one "." character then we probably won't split properly. 
                    // This may be a parse error anyway, or possibly a ScriptFile - which will not use these sections either.
                }
                else
                {
                    myObjectNameFirstPart = String.Empty;  // Some objects may not have a schema.
                    myObjectNameSecondPart = myObjectName;
                }

                // Parse the resulting object name
                switch (myObjectString.Substring(0, cdelim).ToLower())
                {
                    case "db":
                        ObjectType = SQLObjectType.Database;
                        break;

                    case "scriptfile":
                        ObjectType = SQLObjectType.ScriptFile;
                        break;

                    case "table":
                        ObjectType = SQLObjectType.Table;
                        break;

                    case "view":
                        ObjectType = SQLObjectType.View;
                        break;

                    case "storedprocedure":
                        ObjectType = SQLObjectType.StoredProcedure;
                        break;

                    case "partitionscheme":
                        ObjectType = SQLObjectType.PartitionScheme;
                        break;

                    case "partitionfunction":
                        ObjectType = SQLObjectType.PartitionFunction;
                        break;

                    case "schema":
                        ObjectType = SQLObjectType.Schema;
                        break;

                    case "filegroup":
                        ObjectType = SQLObjectType.FileGroup;
                        break;

                    case "user":
                        ObjectType = SQLObjectType.User;
                        break;

                    case "function":
                        ObjectType = SQLObjectType.Function;
                        break;

                    default:
                        ObjectType = SQLObjectType.ParseError;
                        break;
                }

                // Now that we know what kind of object it is, fill in the remaining fields for that type of object.
                switch (ObjectType)
                {
                    case SQLObjectType.Database:
                        ServerName = myObjectNameFirstPart.Trim();
                        DatabaseName = myObjectNameSecondPart.Trim();
                        break;

                    case SQLObjectType.ScriptFile:
                        ScriptFile = myObjectName.Trim();  // Full name
                        break;

                    case SQLObjectType.ParseError:
                        Name = myObjectString;  // Full string for error reporting.
                        break;

                    case SQLObjectType.User:
                        Name = myObjectName.Trim();  // Full name for user objects, since they can contain . characters.
                        break;

                    default:
                        Schema = myObjectNameFirstPart;
                        Name = myObjectNameSecondPart;
                        break;
                }
            }
            else
            {
                // Send a parse error back to the caller
                ObjectType = SQLObjectType.ParseError;
                Schema = String.Empty;
                Name = myObjectString;                       // Put the whole string in the name for error reporting.
            }
        }
    }


    class SQLConnection
    {
        public Server connServer;
        public Database connDatabase;
        public Scripter scripter;

        public SQLConnection(SQLObjectName myConnectionObject)
        {
            try
            {
                connServer = new Server(myConnectionObject.ServerName);
                connDatabase = connServer.Databases[myConnectionObject.DatabaseName];
                scripter = new Scripter(connServer);
                // Set Scripter to SQL 2012 target.  (Doesn't help much since it still generates SQL 2014+ options)
                scripter.Options.TargetServerVersion = Microsoft.SqlServer.Management.Smo.SqlServerVersion.Version110;

                Console.WriteLine("Connected to: " + connServer.Name + " - " + connDatabase.Name);
            }
            catch (Exception e)
            {
                Console.WriteLine("*** Error: Unable to connect to database.");
                Console.WriteLine(e.InnerException.Message);
                Console.WriteLine(e.Message);
            }
        }

        public SQLConnection()
        {
            // Do nothing on default constructor.
        }

        public void ExtractScript(SQLObjectName oname, SQLScripts SQLScriptsCollection, bool Verbose)
        {
            // Store extracted scripts.  Each extract may include multiple scripts.
            StringCollection OutputScripts = new StringCollection();
            string FinalScript = String.Empty;

            switch (oname.ObjectType)
            {
                case SQLObjectType.Table:
                    Microsoft.SqlServer.Management.Smo.Table scriptTable = connDatabase.Tables[oname.Name, oname.Schema];

                    if (scriptTable != null)
                    {
                        StringCollection CheckScripts = new StringCollection();     // Store scripts to be checked
                        String TableScript = String.Empty;                          // Stores individual script for output collection.

                        ScriptingOptions scriptOptions = new ScriptingOptions();
                        scriptOptions.DriAll = true;
                        scriptOptions.Statistics = true;
                        scriptOptions.ClusteredIndexes = true;
                        scriptOptions.NonClusteredIndexes = true;
                        scriptOptions.DriAllConstraints = true;
                        scriptOptions.WithDependencies = false;

                        // Get table and related scripts
                        CheckScripts = scriptTable.Script(scriptOptions);

                        // Check scripts so we can remove invalide SQL 2012 column store options from the script.  
                        // (Why doesn't the target server version remove this?  
                        // This is a crappy place to do this, and it's version specific.  
                        // Need to implement the new versioning code to check target model.
                        foreach (string CheckCCI in CheckScripts)
                        {
                            if (CheckCCI.Contains(", DATA_COMPRESSION = COLUMNSTORE"))
                            {
                                TableScript = CheckCCI.Replace(", DATA_COMPRESSION = COLUMNSTORE", "");
                            }
                            else
                            {
                                TableScript = CheckCCI;
                            }

                            // Add the script into the OutputScripts collection.
                            OutputScripts.Add(TableScript);
                        }

                    }
                    break;

                case SQLObjectType.View:
                    Microsoft.SqlServer.Management.Smo.View scriptView = connDatabase.Views[oname.Name, oname.Schema];

                    if (scriptView != null)
                    {
                        ScriptingOptions scriptOptions = new ScriptingOptions();
                        scriptOptions.DriAll = true;
                        scriptOptions.ClusteredIndexes = true;
                        scriptOptions.NonClusteredIndexes = true;
                        scriptOptions.WithDependencies = false;
                        // Must specify tables seperatly, but safer to do so
                        //   to avoid having duplicate table names in the model.

                        OutputScripts = scriptView.Script(scriptOptions);
                    }
                    break;

                case SQLObjectType.StoredProcedure:
                    Microsoft.SqlServer.Management.Smo.StoredProcedure scriptStoredProcedure = connDatabase.StoredProcedures[oname.Name, oname.Schema];

                    if (scriptStoredProcedure != null)
                    {
                        ScriptingOptions scriptOptions = new ScriptingOptions();
                        scriptOptions.WithDependencies = false;

                        OutputScripts = scriptStoredProcedure.Script(scriptOptions);
                    }
                    break;

                case SQLObjectType.PartitionScheme:
                    {
                        Microsoft.SqlServer.Management.Smo.PartitionScheme scriptPScheme = connDatabase.PartitionSchemes[oname.Name];

                        if (scriptPScheme != null)
                        {
                            ScriptingOptions scriptOptions = new ScriptingOptions();
                            scriptOptions.WithDependencies = false;

                            OutputScripts = scriptPScheme.Script(scriptOptions);
                        }
                    }
                    break;

                case SQLObjectType.PartitionFunction:
                    {
                        Microsoft.SqlServer.Management.Smo.PartitionFunction scriptPFunction = connDatabase.PartitionFunctions[oname.Name];

                        if (scriptPFunction != null)
                        {
                            ScriptingOptions scriptOptions = new ScriptingOptions();
                            scriptOptions.WithDependencies = false;

                            OutputScripts = scriptPFunction.Script(scriptOptions);
                        }

                    }
                    break;

                case SQLObjectType.Schema:
                    {
                        Microsoft.SqlServer.Management.Smo.Schema scriptSchema = connDatabase.Schemas[oname.Name];

                        if (scriptSchema != null)
                        {
                            ScriptingOptions scriptOptions = new ScriptingOptions();
                            scriptOptions.WithDependencies = false;
                            scriptOptions.ScriptOwner = true;  // This includes the "with authorize" part.

                            OutputScripts = scriptSchema.Script(scriptOptions);
                        }
                    }
                    break;

                case SQLObjectType.FileGroup:
                    {
                        Microsoft.SqlServer.Management.Smo.FileGroup scriptFG = connDatabase.FileGroups[oname.Name];

                        if (scriptFG != null)
                        {
                            // Create manual script for FileGroups
                            OutputScripts.Add("ALTER DATABASE [$(DatabaseName)] ADD FILEGROUP " + scriptFG.Name);
                        }
                    }
                    break;

                case SQLObjectType.User:
                    {
                        Microsoft.SqlServer.Management.Smo.User scriptUser = connDatabase.Users[oname.Name];

                        if (scriptUser != null)
                        {
                            ScriptingOptions scriptOptions = new ScriptingOptions();
                            scriptOptions.WithDependencies = false;

                            OutputScripts = scriptUser.Script(scriptOptions);
                        }
                    }
                    break;

                case SQLObjectType.Function:
                    Microsoft.SqlServer.Management.Smo.UserDefinedFunction userDefinedFunction = connDatabase.UserDefinedFunctions[oname.Name, oname.Schema];

                    if (userDefinedFunction != null)
                    {
                        ScriptingOptions scriptOptions = new ScriptingOptions();
                        scriptOptions.WithDependencies = false;

                        OutputScripts = userDefinedFunction.Script(scriptOptions);
                    }
                    break;
            }

            if (OutputScripts.Count > 0)
            {
                Console.WriteLine("Extracted SQL script: (" + oname.ObjectType.ToString() + ") " + ((oname.Schema != String.Empty) ? oname.Schema + "." + oname.Name : oname.Name));

                foreach (string script in OutputScripts)
                {
                    // Add the script to the script collection.
                    FinalScript = FinalScript + script + Environment.NewLine + "GO" + Environment.NewLine;
                }
            }
            else
            {
                Console.WriteLine("Warning - Could not retrieve: (" + oname.ObjectType.ToString() + ") " + ((oname.Schema != String.Empty) ? oname.Schema + "." + oname.Name : oname.Name));

                FinalScript = String.Empty;
            }

            if (FinalScript != String.Empty)
            {
                SQLScriptsCollection.Scripts.Add(FinalScript);
            }
            else
            {
                SQLScriptsCollection.MissingScripts.Add("Missing SQL object: (" + oname.ObjectType.ToString() + ") " + ((oname.Schema != String.Empty) ? oname.Schema + "." + oname.Name : oname.Name));
            }

            // Print script(s) if verbose is on. 
            if (Verbose)
            {
                Console.WriteLine(FinalScript);
            }
        }
    }

    class SQLScripts
    {
        public StringCollection Scripts = new StringCollection();            // Store of all scripts retrived from file and SQL server.
        public StringCollection MissingScripts = new StringCollection();     // List of missing script files, or SQL objects.

        public SQLScripts()
        {
            // Do nothing on default constructor
        }
    }


    class Program
    {

        private static bool PrintVerboseInfo = false;  // If set to true (-- verbose commandline arg) then print out actual scripts retrieved from files and SQL.

        private static SQLScripts mySQLScripts = new SQLScripts();
        //private static StringCollection SQLScripts = new StringCollection();            // Store of all scripts retrived from file and SQL server.
        //private static StringCollection MissingSQLScripts = new StringCollection();     // List of missing script files, or SQL objects.

        static void Main(string[] args)
        {
            string DacpacOutputFilename = string.Empty;
            string DBListFilename = string.Empty;
            string SQLVersion = string.Empty;
            DacPacVersion DacPacVersion = DacPacVersion.SQL2016;

            CommandLineParser.Arguments CommandLine = new CommandLineParser.Arguments(args);

            if (CommandLine["dacpac"] != null)
            {
                DacpacOutputFilename = CommandLine["dacpac"];
            }

            if (CommandLine["objectlist"] != null)
            {
                DBListFilename = CommandLine["objectlist"];
            }

            if (CommandLine["verbose"] != null)
            {
                PrintVerboseInfo = true;
            }

            if (CommandLine["sqlversion"] != null)
            {
                SQLVersion = CommandLine["sqlversion"];

                switch (SQLVersion.ToUpper())
                {
                    case "SQL2012":
                        DacPacVersion = DacPacVersion.SQL2012;
                        break;
                    case "SQL2014":
                        DacPacVersion = DacPacVersion.SQL2014;
                        break;
                    case "SQL2016":
                        DacPacVersion = DacPacVersion.SQL2016;
                        break;
                    case "SQL2017":
                        DacPacVersion = DacPacVersion.SQL2016;
                        break;
                    default:
                        DacPacVersion = DacPacVersion.SQL2016;
                        break;
                }
            }

            // If no Dacpac output file is specified, or both DBlist and FileList are
            // not specified then output usage info.
            if (DacpacOutputFilename == String.Empty || DBListFilename == String.Empty)
            {
                PrintUsage();
            }
            else
            {
                // Check the Dacoutput path
                DacpacOutputFilename = CheckPathDirectory(DacpacOutputFilename);

                // Collect SQL Scripts from DB
                if (DBListFilename != String.Empty)
                {
                    DBListFilename = CheckPathDirectory(DBListFilename);

                    GetDBScripts(DBListFilename);
                }

                // Show scripts we were unable to read or retrieve if there are any
                if (mySQLScripts.MissingScripts.Count > 0)
                {
                    Console.WriteLine("\n***** Script reading errors and warnings:");
                    foreach (string MissingObject in mySQLScripts.MissingScripts)
                    {
                        Console.WriteLine("***  " + MissingObject);
                    }

                }

                // Build DacPac
                BuildDac(DacpacOutputFilename, DacPacVersion);

                // End
                Console.WriteLine("\nProcess Complete.");
            }
        }

        private static void PrintUsage()
        {
            // Usage:
            Console.WriteLine("Usage: DacMini -dacpac:<DacpacOutputFile> -objectlist:<ObjectListFile> [-verbose] [-sqlversion]");
            Console.WriteLine("\nObjectListFile Format:");
            Console.WriteLine("DB:<SQLServerName>.<DatabaseName>     (Initialise connection to server and database)");
            Console.WriteLine("<objecttype>:<schema>.<name1>         (for objects with schemas)");
            Console.WriteLine("<objecttype>:<name2>                  (for objects without schemas)");
            Console.WriteLine("ScriptFile:<FilePath>                 (File containing SQL script/s)");
            Console.WriteLine("\nNotes:\nLines can be commented with -- or // at the start of a line.");
            Console.WriteLine("DB: is required to initialise a connection to database in order to retrieve subsequent SQL Objects.");
            Console.WriteLine("Objects can be retrieved from multiple servers and/or databases by adding another DB: connection line.");
            Console.WriteLine("The verbose option will print retrieved scripts to the console.");
            Console.WriteLine("SQLVersion can be one of the following: SQL2012, SQL2014, SQL2016.  Default is SQL2012");
            Console.WriteLine("\nSupported SQL Object Types:");

            foreach (SQLObjectType ot in Enum.GetValues(typeof(SQLObjectType)))
            {
                if (ot != SQLObjectType.Database && ot != SQLObjectType.ParseError && ot != SQLObjectType.ScriptFile)
                {
                    Console.WriteLine(" - " + ot.ToString());
                }
            }
        }

        private static void GetScriptFile(string ScriptFilename)
        {
            string script = string.Empty;

            try
            {
                // Check to see if file exists
                if (File.Exists(ScriptFilename))
                {
                    // Get the script from the file
                    Console.WriteLine("Adding script/s from file: " + ScriptFilename);
                    script = File.ReadAllText(ScriptFilename).ToString();

                    // Add the script to the SQLScripts collection
                    mySQLScripts.Scripts.Add(script);

                    // Show verbose info if requested
                    if (PrintVerboseInfo)
                    {
                        Console.WriteLine(script);
                    }
                }
                else
                {
                    Console.WriteLine("Warning - Could not retrieve: (ScriptFile) " + ScriptFilename);
                    mySQLScripts.MissingScripts.Add("Script file does not exist: " + ScriptFilename);
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("**** Error: " + e.Message);
            }
        }

        private static void GetDBScripts(string DBTablesListFilename)
        {
            Console.WriteLine("------- Extracting scripts from database/s and/or files -------");
            // Get the list of SQL script files we want to combine into this model
            try
            {
                string contents = File.ReadAllText(DBTablesListFilename);

                // Split the list of files into an array.
                string[] scriptobjects = contents.Split(new string[] { Environment.NewLine }, StringSplitOptions.RemoveEmptyEntries);

                // Setup a new SQLObjectName object.
                SQLObjectName oname = new SQLObjectName(String.Empty);

                string outputScript = string.Empty;

                SQLConnection sconn = new SQLConnection();

                // Check through each line for DB instruction, or object name.
                foreach (string scriptobject in scriptobjects)
                {
                    // Check for commented out lines
                    if ((scriptobject.StartsWith(@"--") || scriptobject.StartsWith(@"//")) == false)
                    {
                        oname = new SQLObjectName(scriptobject);

                        if (oname.ObjectType != SQLObjectType.ParseError)
                        {
                            if (oname.ObjectType == SQLObjectType.Database)
                            {
                                // Set up new database connection object.
                                sconn = new SQLConnection(oname);
                            }
                            else if (oname.ObjectType == SQLObjectType.ScriptFile)
                            {
                                // Extract script from File
                                GetScriptFile(oname.ScriptFile);

                            }
                            else
                            {
                                // Extract script from SQL Server
                                if (sconn.connDatabase != null)
                                {
                                    sconn.ExtractScript(oname, mySQLScripts, PrintVerboseInfo);
                                }
                                else
                                {
                                    Console.WriteLine("Warning - Could not retrieve: (" + oname.ObjectType.ToString() + ") - " + ((oname.Schema != String.Empty) ? oname.Schema + "." + oname.Name : oname.Name));
                                    mySQLScripts.MissingScripts.Add("No db connection for object: (" + oname.ObjectType.ToString() + ") - " + ((oname.Schema != String.Empty) ? oname.Schema + "." + oname.Name : oname.Name));
                                }
                            }
                        }
                        else
                        {
                            mySQLScripts.MissingScripts.Add("Unable to parse SQL object from string: " + oname.Name);
                        }
                    }
                }
            }
            catch (System.IO.FileNotFoundException e)
            {
                Console.WriteLine("**** File not found error: " + DBTablesListFilename);
                Console.WriteLine("**** Error: " + e.Message);
            }
            catch (Exception e)
            {
                Console.WriteLine("**** Error: " + e.Message);
            }
        }

        private static string CheckPathDirectory(string testPath)
        {
            if (Path.IsPathRooted(testPath) == false)
            {
                // Add current directory if it's not a full path
                testPath = Path.Combine(Environment.CurrentDirectory, testPath);
            }

            return testPath;
        }

        static void BuildDac(string dacpacPath, DacPacVersion dacpacVersion)
        {
            Console.WriteLine("\n------- Building DacPac -------");

            if (mySQLScripts.Scripts.Count >= 0)
            {

                // Figure out which version we want to build.

                Microsoft.SqlServer.Dac.Model.SqlServerVersion modelVersion;
                switch (dacpacVersion)
                {
                    case DacPacVersion.SQL2012:
                        modelVersion = Microsoft.SqlServer.Dac.Model.SqlServerVersion.Sql110;
                        break;

                    case DacPacVersion.SQL2014:
                        modelVersion = Microsoft.SqlServer.Dac.Model.SqlServerVersion.Sql120;
                        break;

                    case DacPacVersion.SQL2016:
                        modelVersion = Microsoft.SqlServer.Dac.Model.SqlServerVersion.Sql130;
                        break;

                    default:
                        modelVersion = Microsoft.SqlServer.Dac.Model.SqlServerVersion.Sql110;
                        break;
                }

                using (TSqlModel model = new TSqlModel(modelVersion, new TSqlModelOptions { }))
                {
                    foreach (string SQLScript in mySQLScripts.Scripts)
                    {
                        // Add it to the model
                        model.AddObjects(SQLScript);
                    }

                    // Now write the model
                    try
                    {
                        DacPackageExtensions.BuildPackage(
                            dacpacPath,
                            model,
                            new PackageMetadata { Name = "Mini_DacPac", Description = "Built by DacMini.", Version = "1.0" },
                            new PackageOptions()
                            );

                        Console.WriteLine("DAC package created.");
                    }
                    catch (Microsoft.SqlServer.Dac.DacServicesException e)
                    {
                        Console.WriteLine("DAC creation failed:");
                        Console.WriteLine("Error: " + e.Message);
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("DAC creation may have failed:");
                        Console.WriteLine("Error: " + e.Message);
                    }
                }
            }
            else
            {
                Console.WriteLine("Warning - No scripts were found to package into DacPac.");
            }
        }
    }
}
