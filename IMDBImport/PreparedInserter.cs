using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IMDBImport;
using IMDBImport.Models;
using IMDbImportData.Models;
using Microsoft.Data.SqlClient;

namespace IMDbImportData.Inserters
{
    public class PreparedInserter : IInserter
    {
        public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
        {
            string query = "INSERT INTO Genres (" +
                "TConst, " +
                "Genre) " +
                    "VALUES (@TConst, @Genre)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.Prepare();
            foreach (GenreModel genre in genres)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", genre.TConst);
                sqlComm.Parameters.AddWithValue("@Genre", genre.Genre);
                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Error inserting query:\r\n" + query + " " + ex.Message);
                }
            }
        }

        public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
        {
            string query = "INSERT INTO Names (" +
                            "NConst, " +
                            "PrimaryName, " +
                            "BirthYear, " +
                            "DeathYear) " +
                                "VALUES (@NConst, @PrimaryName, @BirthYear, @DeathYear)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.Prepare();
            foreach (Name_Model name in names)
            {
                sqlComm.Prepare();
                sqlComm.Parameters.AddWithValue("@NConst", name.NConst);
                sqlComm.Parameters.AddWithValue("@PrimaryName", name.PrimaryName);
                sqlComm.Parameters.AddWithValue("@BirthYear", (object)name.BirthYear ?? DBNull.Value);
                sqlComm.Parameters.AddWithValue("@DeathYear", (object)name.DeathYear ?? DBNull.Value);
                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException sqlex)
                {
                    Console.WriteLine("Error inserting query:\r\n" + query);
                }
            }
        }

        public void InsertNameProfessions(List<NameProfession_Model> professions, SqlConnection sqlConn)
        {
            string query = "INSERT INTO Name_Professions (" +
                            "NConst, " +
                            "PrimaryProfession) " +
                                "VALUES (@NConst, @PrimaryProfession)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.Prepare();
            foreach (NameProfession_Model profession in professions)
            {
                sqlComm.Prepare();
                sqlComm.Parameters.AddWithValue("@NConst", profession.NConst);
                sqlComm.Parameters.AddWithValue("@PrimaryProfession", profession.PrimaryProfession);
                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException sqlex)
                {
                    Console.WriteLine("Error inserting query:\r\n" + query);
                }
            }
        }

        public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
        {
            string query = "INSERT INTO Titles (" +
                    "TConst, " +
                    "TitleType, " +
                    "PrimaryTitle, " +
                    "OriginalTitle, " +
                    "IsAdult, " +
                    "StartYear, " +
                    "EndYear, " +
                    "RunTimeMinutes) " +
                                "VALUES (@TConst, @TitleType, @PrimaryTitle, " +
                                "@OriginalTitle, @IsAdult, @StartYear, @EndYear, " +
                                "@RunTimeMinutes)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.Prepare();
            foreach (TitleModel movie in titles)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", movie.TConst);
                sqlComm.Parameters.AddWithValue("@TitleType", movie.TitleType);
                sqlComm.Parameters.AddWithValue("@PrimaryTitle", movie.PrimaryTitle);
                sqlComm.Parameters.AddWithValue("@OriginalTitle", movie.OriginalTitle);
                sqlComm.Parameters.AddWithValue("@IsAdult", movie.IsAdult);
                sqlComm.Parameters.AddWithValue("@StartYear", (object)movie.StartYear ?? DBNull.Value);
                sqlComm.Parameters.AddWithValue("@EndYear", (object)movie.EndYear ?? DBNull.Value);
                sqlComm.Parameters.AddWithValue("@RuntimeMinutes", (object)movie.RunTimeMinutes ?? DBNull.Value);
                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine("Error inserting query:\r\n" + query);
                }
            }
        }

        public void InsertNameTitles(List<NameTitle_Model> nameTitleModels, SqlConnection sqlConn)
        {
            string query = "INSERT INTO Name_Titles (" +
                            "NConst, " +
                            "TConst) " +
                                "VALUES (@NConst, @TConst)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.Prepare();
            foreach (NameTitle_Model nameTitle in nameTitleModels)
            {
                sqlComm.Prepare();
                sqlComm.Parameters.AddWithValue("@NConst", nameTitle.NConst);
                sqlComm.Parameters.AddWithValue("@TConst", nameTitle.TConst);
                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException sqlex)
                {
                    Console.WriteLine("Error inserting query:\r\n" + query);
                }
            }
        }

        public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

    }
}