using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Diagnostics;

public class ImportData
{
    public void RunImport()
    {
        Console.OutputEncoding = Encoding.UTF8;
        Console.WriteLine("IMDB Import\n");

        const int LIMIT = 100000;

        Stopwatch totalTimer = Stopwatch.StartNew();
        IInserter bulkInserter = new BulkInserter();

        Console.WriteLine("LOADING AND INSERTING: title.basics.tsv...");
        LoadAndInsertTitles(bulkInserter, LIMIT);

        Console.WriteLine("\nLOADING AND INSERTING: name.basics.tsv...");
        LoadAndInsertNames(bulkInserter, LIMIT);

        Console.WriteLine("\nLOADING AND INSERTING: title.crew.tsv...");
        LoadAndInsertCrew(bulkInserter, LIMIT);

        totalTimer.Stop();
        Console.WriteLine($"\n✓ Completed in {totalTimer.Elapsed.TotalMinutes:F1} minutes");

        ValidateImport();

        Console.WriteLine("\nImport successful!");
    }

    private void ValidateImport()
    {
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
        }
    }

    private void LoadAndInsertTitles(IInserter bulkInserter, int limit)
    {
        string filePath = "C:\\IMDB_temp\\title.basics.tsv";
        if (!File.Exists(filePath)) return;

        List<Title_Model> titles = new List<Title_Model>();
        List<Genre_Model> genres = new List<Genre_Model>();
        int totalImported = 0;

        using (SqlConnection sqlConn = new SqlConnection(
            "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
        {
            sqlConn.Open();
            StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
            reader.ReadLine();

            string line;
            while ((line = reader.ReadLine()) != null && totalImported < limit)
            {
                string[] parts = line.Split('\t');
                if (parts.Length == 9 && parts[1] != "\\N")
                {
                    titles.Add(new Title_Model(parts));
                    totalImported++;

                    if (parts[8] != "\\N")
                    {
                        foreach (string g in parts[8].Split(','))
                            genres.Add(new Genre_Model(new[] { parts[0], g }));
                    }
                }

                if (titles.Count >= 100000)
                {
                    bulkInserter.InsertTitles(titles, sqlConn);
                    bulkInserter.InsertGenres(genres, sqlConn);
                    titles.Clear();
                    genres.Clear();
                    Console.Write(".");
                }
            }

            if (titles.Count > 0)
            {
                bulkInserter.InsertTitles(titles, sqlConn);
                bulkInserter.InsertGenres(genres, sqlConn);
            }
        }
    }

    private void LoadAndInsertNames(IInserter bulkInserter, int limit)
    {
        string filePath = "C:\\IMDB_temp\\name.basics.tsv";
        if (!File.Exists(filePath)) return;

        List<Name_Model> names = new List<Name_Model>();
        List<NameProfession_Model> profs = new List<NameProfession_Model>();
        int totalImported = 0;

        using (SqlConnection sqlConn = new SqlConnection(
            "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
        {
            sqlConn.Open();
            StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
            reader.ReadLine();

            string line;
            while ((line = reader.ReadLine()) != null && totalImported < limit)
            {
                string[] parts = line.Split('\t');
                if (parts.Length > 4 && parts[4] != "\\N")
                {
                    names.Add(new Name_Model(parts));
                    totalImported++;

                    foreach (string p in parts[4].Split(','))
                        profs.Add(new NameProfession_Model(new[] { parts[0], p }));
                }

                if (names.Count >= 100000)
                {
                    bulkInserter.InsertNames(names, sqlConn);
                    bulkInserter.InsertNameProfessions(profs, sqlConn);
                    names.Clear();
                    profs.Clear();
                    Console.Write(".");
                }
            }

            if (names.Count > 0)
            {
                bulkInserter.InsertNames(names, sqlConn);
                bulkInserter.InsertNameProfessions(profs, sqlConn);
            }
        }
    }

    private void LoadAndInsertCrew(IInserter bulkInserter, int limit)
    {
        string filePath = "C:\\IMDB_temp\\title.crew.tsv";
        if (!File.Exists(filePath)) return;

        List<CrewDirector_Model> dirs = new List<CrewDirector_Model>();
        List<CrewWriter_Model> writ = new List<CrewWriter_Model>();
        int totalImported = 0;

        using (SqlConnection sqlConn = new SqlConnection(
            "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;"))
        {
            sqlConn.Open();
            StreamReader reader = new StreamReader(filePath, Encoding.UTF8);
            reader.ReadLine();

            string line;
            while ((line = reader.ReadLine()) != null && totalImported < limit)
            {
                string[] parts = line.Split('\t');
                if (parts.Length >= 3 && parts[0] != "\\N")
                {
                    if (parts[1] != "\\N")
                        foreach (string d in parts[1].Split(','))
                            dirs.Add(new CrewDirector_Model(new[] { parts[0], d }));

                    if (parts[2] != "\\N")
                        foreach (string w in parts[2].Split(','))
                            writ.Add(new CrewWriter_Model(new[] { parts[0], w }));

                    totalImported++;
                }

                if (dirs.Count >= 100000 || writ.Count >= 100000)
                {
                    bulkInserter.InsertCrewDirectors(dirs, sqlConn);
                    bulkInserter.InsertCrewWriters(writ, sqlConn);
                    dirs.Clear();
                    writ.Clear();
                    Console.Write(".");
                }
            }

            if (dirs.Count > 0 || writ.Count > 0)
            {
                bulkInserter.InsertCrewDirectors(dirs, sqlConn);
                bulkInserter.InsertCrewWriters(writ, sqlConn);
            }
        }
    }

    private int ExecuteCountQuery(SqlConnection connection, string query)
    {
        using (SqlCommand cmd = new SqlCommand(query, connection))
        {
            return (int?)cmd.ExecuteScalar() ?? 0;
        }
    }
}