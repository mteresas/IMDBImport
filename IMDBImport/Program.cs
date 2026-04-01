using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;

// what we are working with
Console.WriteLine("IMDB Import");

// list
List<Title_Model> movies = new List<Title_Model>();
List<Genre_Model> genres = new List<Genre_Model>();

// locate file from drive
foreach (string movie in File.ReadLines("C:/temp/title.basics.tsv").Skip(1).Take(10))
{
	string[] movieParts = movie.Split('\t');
	string[] genreParts = new string[2];
	if (movieParts.Length == 9)
	{
		movies.Add(new Title_Model(movieParts));

		string genreString = movieParts[8]; // reads the 8th part/index
		try
		{
			string[] genreArray = genreString.Split(','); // string splits into an array of genres
			foreach (string genre in genreArray)
			{
				genreParts[0] = movieParts[0]; // TConst
				genreParts[1] = genre; // Genre
				genres.Add(new Genre_Model(genreParts));
			}
		}
		catch (Exception)
		{
			genreParts[0] = movieParts[0]; // TConst
			genreParts[1] = movieParts[8]; // Genre (if only one genre, no comma, so it will be added as is)
			genres.Add(new Genre_Model(genreParts));
		}
	}
	else
	{
		Console.WriteLine("Invalid line: " + movie);
	}
}

/*
// print movies
foreach (var movie in movies)
{
	Console.WriteLine(movie.ToString());
}
*/

// insert actual data
//IInserter normalInserter = new NormalInserter();
IInserter preparedInserter = new PreparedInserter();
//IInserter bulkInserter = new BulkInserter();

SqlConnection sqlConn = new SqlConnection(
	"Server=localhost;Database=IMDB;Trusted_Connection=True;" +
	"Trusted_Connection=True;TrustServerCertificate=True;");

//sqlConn.Open();
//normalInserter.InsertTitles(movies, sqlConn);
//sqlConn.Close();

sqlConn.Open();
//preparedInserter.InsertTitles(movies, sqlConn);
preparedInserter.InsertGenres(genres, sqlConn);
sqlConn.Close();

//sqlConn.Open();
//bulkInserter.InsertTitles(movies, sqlConn);
//sqlConn.Close();