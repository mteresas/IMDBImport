using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;

Console.WriteLine("IMDB Import");

List<Title_Model> titles = new List<Title_Model>();
List<Genre_Model> genres = new List<Genre_Model>();
List<Name_Model> names = new List<Name_Model>();
List<NameProfession_Model> nameProfessions = new List<NameProfession_Model>();
List<CrewDirector_Model> crewDirectors = new List<CrewDirector_Model>();
List<CrewWriter_Model> crewWriters = new List<CrewWriter_Model>();

foreach (string line in File.ReadLines("C:/temp/title.basics.tsv").Skip(1))
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
    else
    {
        Console.WriteLine("Invalid line: " + line);
    }
}

foreach (string line in File.ReadLines("C:/temp/name.basics.tsv").Skip(1))
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
    else
    {
        Console.WriteLine("Invalid line: " + line);
    }
}

foreach (string line in File.ReadLines("C:/temp/title.crew.tsv").Skip(1))
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
    else
    {
        Console.WriteLine("Invalid line: " + line);
    }
}

IInserter bulkInserter = new BulkInserter();

SqlConnection sqlConn = new SqlConnection(
    "Server=localhost;Database=IMDB;Trusted_Connection=True;" +
    "Trusted_Connection=True;TrustServerCertificate=True;");

sqlConn.Open();

bulkInserter.InsertTitles(titles, sqlConn);
bulkInserter.InsertGenres(genres, sqlConn);
bulkInserter.InsertNames(names, sqlConn);
bulkInserter.InsertNameProfessions(nameProfessions, sqlConn);
bulkInserter.InsertCrewDirectors(crewDirectors, sqlConn);
bulkInserter.InsertCrewWriters(crewWriters, sqlConn);

sqlConn.Close();