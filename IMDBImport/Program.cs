using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

try
{
    Console.OutputEncoding = Encoding.UTF8;
    Console.WriteLine("IMDB Import - Start\n");

    List<Title_Model> titles = new List<Title_Model>();
    List<Genre_Model> genres = new List<Genre_Model>();
    List<Name_Model> names = new List<Name_Model>();
    List<NameProfession_Model> nameProfessions = new List<NameProfession_Model>();
    List<CrewDirector_Model> crewDirectors = new List<CrewDirector_Model>();
    List<CrewWriter_Model> crewWriters = new List<CrewWriter_Model>();

    Console.WriteLine("Loading title.basics.tsv...");
    foreach (string line in File.ReadLines("C:\\IMDB\\IMDB\\title.basics.tsv", Encoding.UTF8).Skip(1))
    {
        string[] parts = line.Split('\t');
        if (parts.Length == 9)
        {
            titles.Add(new Title_Model(parts));
            string genreString = parts[8];
            try
            {
                string[] genreArray = genreString.Split(',');
                foreach (string genre in genreArray)
                {
                    genres.Add(new Genre_Model(new[] { parts[0], genre }));
                }
            }
            catch (Exception)
            {
                genres.Add(new Genre_Model(new[] { parts[0], parts[8] }));
            }
        }
    }
    Console.WriteLine($"  ✓ {titles.Count:N0} titles loaded\n");

    Console.WriteLine("Loading name.basics.tsv...");
    foreach (string line in File.ReadLines("C:\\IMDB\\IMDB\\name.basics.tsv", Encoding.UTF8).Skip(1))
    {
        string[] parts = line.Split('\t');
        if (parts.Length >= 4)
        {
            names.Add(new Name_Model(parts));
            if (parts.Length > 4 && parts[4] != "\\N")
            {
                string[] professionArray = parts[4].Split(',');
                foreach (string profession in professionArray)
                {
                    nameProfessions.Add(new NameProfession_Model(new[] { parts[0], profession }));
                }
            }
        }
    }
    Console.WriteLine($"{names.Count:N0} names loaded\n");

    Console.WriteLine("Loading title.crew.tsv...");
    foreach (string line in File.ReadLines("C:\\IMDB\\IMDB\\title.crew.tsv", Encoding.UTF8).Skip(1))
    {
        string[] parts = line.Split('\t');
        if (parts.Length >= 3)
        {
            string directorString = parts[1];
            if (directorString != "\\N")
            {
                string[] directorArray = directorString.Split(',');
                foreach (string director in directorArray)
                {
                    crewDirectors.Add(new CrewDirector_Model(new[] { parts[0], director }));
                }
            }
            string writerString = parts[2];
            if (writerString != "\\N")
            {
                string[] writerArray = writerString.Split(',');
                foreach (string writer in writerArray)
                {
                    crewWriters.Add(new CrewWriter_Model(new[] { parts[0], writer }));
                }
            }
        }
    }
    Console.WriteLine($" {crewDirectors.Count:N0} directors, {crewWriters.Count:N0} writers loaded\n");

    Console.WriteLine("Inserting data into database...\n");

    IInserter bulkInserter = new BulkInserter();
    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();
        Console.WriteLine("✓ Connected to database\n");

        bulkInserter.InsertTitles(titles, sqlConn);
        bulkInserter.InsertGenres(genres, sqlConn);
        bulkInserter.InsertNames(names, sqlConn);
        bulkInserter.InsertNameProfessions(nameProfessions, sqlConn);
        bulkInserter.InsertCrewDirectors(crewDirectors, sqlConn);
        bulkInserter.InsertCrewWriters(crewWriters, sqlConn);

        sqlConn.Close();
    }

    Console.WriteLine("\n\nValidating import...\n");

    using (SqlConnection sqlConn = new SqlConnection(
        "Server=localhost;Database=IMDB;Integrated Security=true;TrustServerCertificate=True;"))
    {
        sqlConn.Open();

        int countTitles = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Title");
        int countNames = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Names");
        int countGenres = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Title_Genres");
        int countProfessions = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.Name_Professions");
        int countDirectors = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.CrewDirectors");
        int countWriters = ExecuteCountQuery(sqlConn, "SELECT COUNT(*) FROM dbo.CrewWriters");

        Console.WriteLine($"Titles:       {countTitles:N0}");
        Console.WriteLine($"Names:        {countNames:N0}");
        Console.WriteLine($"Genres:       {countGenres:N0}");
        Console.WriteLine($"Professions:  {countProfessions:N0}");
        Console.WriteLine($"Directors:    {countDirectors:N0}");
        Console.WriteLine($"Writers:      {countWriters:N0}");

        int total = countTitles + countNames + countGenres + countProfessions + countDirectors + countWriters;
        Console.WriteLine($"\nTotal records: {total:N0}");

        sqlConn.Close();
    }

    Console.WriteLine("\n✓ Import successful!");
}
catch (Exception ex)
{
    Console.WriteLine($"\nError: {ex.Message}");
    if (ex.InnerException != null)
        Console.WriteLine($"Details: {ex.InnerException.Message}");
}

Console.WriteLine("\nPress any key to exit...");
Console.ReadKey();

static int ExecuteCountQuery(SqlConnection connection, string query)
{
    using (SqlCommand cmd = new SqlCommand(query, connection))
    {
        return (int)cmd.ExecuteScalar();
    }
}