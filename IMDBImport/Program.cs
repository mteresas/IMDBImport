using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Diagnostics;

try
{
    Console.OutputEncoding = Encoding.UTF8;
    Console.WriteLine("IMDB Import\n");

    const int BATCH_SIZE = 1000000;

    Console.WriteLine($"Batch Size: {BATCH_SIZE:N0} lines");

    Stopwatch totalTimer = Stopwatch.StartNew();
    IInserter bulkInserter = new BulkInserter();

    Console.WriteLine("LOADING AND INSERTING: title.basics.tsv...");
    LoadAndInsertTitles(bulkInserter);

    Console.WriteLine("\nLOADING AND INSERTING: name.basics.tsv...");
    LoadAndInsertNames(bulkInserter);

    Console.WriteLine("\nLOADING AND INSERTING: title.crew.tsv...");
    LoadAndInsertCrew(bulkInserter);

    totalTimer.Stop();
    Console.WriteLine($"\nCompleted in {totalTimer.Elapsed.TotalMinutes:F1} minutes");

    Console.WriteLine("\nVALIDATING IMPORT...\n");
    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();

        Console.WriteLine($"Titles:       {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Title"):N0}");
        Console.WriteLine($"Names:        {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Names"):N0}");
        Console.WriteLine($"Genres:       {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Title_Genres"):N0}");
        Console.WriteLine($"Professions:  {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Name_Professions"):N0}");
        Console.WriteLine($"Directors:    {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.CrewDirectors"):N0}");
        Console.WriteLine($"Writers:      {ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.CrewWriters"):N0}");

        sqlConn.Close();
    }

    Console.WriteLine("\nImport successful!");
}
catch (Exception ex)
{
    Console.WriteLine($"ERROR: {ex.Message}");
}

Console.WriteLine("\nPress any key to exit...");
Console.ReadKey();

static void LoadAndInsertTitles(IInserter bulkInserter)
{
    string filePath = "C:\\IMDB_temp\\title.basics.tsv";
    if (!File.Exists(filePath)) { Console.WriteLine("File not found!"); return; }

    List<Title_Model> titles = new List<Title_Model>(1000000);
    List<Genre_Model> genres = new List<Genre_Model>(3000000);
    int batchCount = 0;
    int skipped = 0;

    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();

        StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
        reader.ReadLine(); 

        string line;
        while ((line = reader.ReadLine()) != null)
        {
            try
            {
                string[] parts = line.Split('\t');
                if (parts.Length == 9)
                {

                    bool isComplete =
                        parts[0] != "\\N" &&  
                        parts[1] != "\\N" &&  
                        parts[2] != "\\N" &&  
                        parts[3] != "\\N" &&  
                        parts[4] != "\\N" &&  
                        parts[5] != "\\N" &&  
                        parts[7] != "\\N";    

                    if (isComplete)
                    {
                        titles.Add(new Title_Model(parts));

                        if (parts[8] != "\\N")
                        {
                            foreach (string g in parts[8].Split(','))
                                genres.Add(new Genre_Model(new[] { parts[0], g }));
                        }
                    }
                    else
                    {
                        skipped++;
                    }
                }
            }
            catch { }

            if (titles.Count >= 1000000)
            {
                bulkInserter.InsertTitles(titles, sqlConn);
                bulkInserter.InsertGenres(genres, sqlConn);
                titles.Clear();
                genres.Clear();
                batchCount++;
                Console.Write(".");
            }
        }

        if (titles.Count > 0)
        {
            bulkInserter.InsertTitles(titles, sqlConn);
            bulkInserter.InsertGenres(genres, sqlConn);
            Console.Write(".");
        }

        reader.Close();
        sqlConn.Close();
    }

    Console.WriteLine($" ({batchCount} batches, {skipped:N0} records skipped)");
}

static void LoadAndInsertNames(IInserter bulkInserter)
{
    string filePath = "C:\\IMDB_temp\\name.basics.tsv";
    if (!File.Exists(filePath)) { Console.WriteLine("File not found!"); return; }

    List<Name_Model> names = new List<Name_Model>(1000000);
    List<NameProfession_Model> profs = new List<NameProfession_Model>(2000000);
    int batchCount = 0;
    int skipped = 0;

    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();

        StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
        reader.ReadLine(); 

        string line;
        while ((line = reader.ReadLine()) != null)
        {
            try
            {
                string[] parts = line.Split('\t');
                if (parts.Length >= 4)
                {

                    bool isComplete =
                        parts[0] != "\\N" &&  
                        parts[1] != "\\N" &&  
                        parts[2] != "\\N";    

                    if (isComplete)
                    {
                        names.Add(new Name_Model(parts));

                        // Professions - OK si NULL
                        if (parts.Length > 4 && parts[4] != "\\N")
                        {
                            foreach (string p in parts[4].Split(','))
                                profs.Add(new NameProfession_Model(new[] { parts[0], p }));
                        }
                    }
                    else
                    {
                        skipped++;
                    }
                }
            }
            catch { }

            if (names.Count >= 1000000)
            {
                bulkInserter.InsertNames(names, sqlConn);
                bulkInserter.InsertNameProfessions(profs, sqlConn);
                names.Clear();
                profs.Clear();
                batchCount++;
                Console.Write(".");
            }
        }

        if (names.Count > 0)
        {
            bulkInserter.InsertNames(names, sqlConn);
            bulkInserter.InsertNameProfessions(profs, sqlConn);
            Console.Write(".");
        }

        reader.Close();
        sqlConn.Close();
    }

    Console.WriteLine($" ({batchCount} batches, {skipped:N0} records skipped)");
}

static void LoadAndInsertCrew(IInserter bulkInserter)
{
    string filePath = "C:\\IMDB_temp\\title.crew.tsv";
    if (!File.Exists(filePath)) { Console.WriteLine("File not found!"); return; }

    List<CrewDirector_Model> dirs = new List<CrewDirector_Model>(2000000);
    List<CrewWriter_Model> writ = new List<CrewWriter_Model>(2000000);
    int batchCount = 0;
    int skipped = 0;

    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();

        StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
        reader.ReadLine(); 

        string line;
        while ((line = reader.ReadLine()) != null)
        {
            try
            {
                string[] parts = line.Split('\t');
                if (parts.Length >= 3)
                {

                    bool isComplete =
                        parts[0] != "\\N" &&                              
                        (parts[1] != "\\N" || parts[2] != "\\N");        

                    if (isComplete)
                    {
                        
                        if (parts[1] != "\\N")
                        {
                            foreach (string d in parts[1].Split(','))
                                dirs.Add(new CrewDirector_Model(new[] { parts[0], d }));
                        }

                        
                        if (parts[2] != "\\N")
                        {
                            foreach (string w in parts[2].Split(','))
                                writ.Add(new CrewWriter_Model(new[] { parts[0], w }));
                        }
                    }
                    else
                    {
                        skipped++;
                    }
                }
            }
            catch { }

            if (dirs.Count >= 1000000 || writ.Count >= 1000000)
            {
                bulkInserter.InsertCrewDirectors(dirs, sqlConn);
                bulkInserter.InsertCrewWriters(writ, sqlConn);
                dirs.Clear();
                writ.Clear();
                batchCount++;
                Console.Write(".");
            }
        }

        if (dirs.Count > 0 || writ.Count > 0)
        {
            bulkInserter.InsertCrewDirectors(dirs, sqlConn);
            bulkInserter.InsertCrewWriters(writ, sqlConn);
            Console.Write(".");
        }

        reader.Close();
        sqlConn.Close();
    }

    Console.WriteLine($" ({batchCount} batches, {skipped:N0} records skipped)");
}

static int ExecuteCountQuery(SqlConnection connection, string query)
{
    using (SqlCommand cmd = new SqlCommand(query, connection))
    {
        cmd.CommandTimeout = 120;
        return (int?)cmd.ExecuteScalar() ?? 0;
    }
}