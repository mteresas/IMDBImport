using Microsoft.Data.SqlClient;
using System;
using System.Data;

public class DBSecurity
{
    private string connectionString =
        "Server=localhost;Database=IMDB26;Integrated Security=true;TrustServerCertificate=True;";


    private int ConvertImdbToInt(string imdbId)
    {
        if (string.IsNullOrWhiteSpace(imdbId))
            throw new Exception("Invalid ID");

        if (imdbId.StartsWith("tt") || imdbId.StartsWith("nm"))
            return int.Parse(imdbId.Substring(2));

        return int.Parse(imdbId);
    }


    public void SearchMovies(string title, bool showId = false)
    {
        using SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();

        using SqlCommand cmd = new SqlCommand("sp_SearchMovies", conn);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Title", title);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            if (showId)
                Console.WriteLine($"ID: {reader["imdbID"]} - {reader["PrimaryTitle"]} ({reader["StartYear"]})");
            else
                Console.WriteLine($"{reader["PrimaryTitle"]} ({reader["StartYear"]})");
        }
    }

    public void SearchPersons(string name, bool showId = false)
    {
        using SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();

        using SqlCommand cmd = new SqlCommand("sp_SearchPersons", conn);
        cmd.CommandType = CommandType.StoredProcedure;
        cmd.Parameters.AddWithValue("@Name", name);

        using SqlDataReader reader = cmd.ExecuteReader();
        while (reader.Read())
        {
            if (showId)
                Console.WriteLine($"ID: {reader["imdbID"]} - {reader["PrimaryName"]}");
            else
                Console.WriteLine($"{reader["PrimaryName"]}");
        }
    }

  
    public void AddMovie(string title, int year)
    {
        using SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();

        using SqlCommand cmd = new SqlCommand("sp_AddMovie", conn);
        cmd.CommandType = CommandType.StoredProcedure;

        cmd.Parameters.AddWithValue("@Title", title);
        cmd.Parameters.AddWithValue("@Year", year);

        using SqlDataReader reader = cmd.ExecuteReader();
        if (reader.Read())
        {
            int id = Convert.ToInt32(reader["NewMovieID"]);
            Console.WriteLine($"Movie added with ID: tt{id:D7}");
        }
    }

    public void UpdateMovie(string imdbId, string title, int year)
    {
        int id = ConvertImdbToInt(imdbId);

        using SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();

        using SqlCommand cmd = new SqlCommand("sp_UpdateMovie", conn);
        cmd.CommandType = CommandType.StoredProcedure;

        cmd.Parameters.AddWithValue("@Tconst", id);
        cmd.Parameters.AddWithValue("@Title", title);
        cmd.Parameters.AddWithValue("@Year", year);

        cmd.ExecuteNonQuery();

        Console.WriteLine("Movie updated!");
    }

   
    public void AddPerson(string name, int year)
    {
        using (SqlConnection conn = new SqlConnection(connectionString))
        {
            conn.Open();

            using (SqlCommand cmd = new SqlCommand("sp_AddPerson", conn))
            {
                cmd.CommandType = CommandType.StoredProcedure;

               
                cmd.Parameters.AddWithValue("@PrimaryName", name);
                cmd.Parameters.AddWithValue("@BirthYear", year);

                cmd.ExecuteNonQuery();
            }
        }
    }

   
    public void DeleteMovie(string imdbId)
    {
        int id = ConvertImdbToInt(imdbId);

        using SqlConnection conn = new SqlConnection(connectionString);
        conn.Open();

        using SqlCommand cmd = new SqlCommand("sp_DeleteMovie", conn);
        cmd.CommandType = CommandType.StoredProcedure;

        cmd.Parameters.AddWithValue("@Tconst", id);

        cmd.ExecuteNonQuery();

        Console.WriteLine("Movie deleted!");
    }
}