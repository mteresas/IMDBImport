using System;
using System.Collections.Generic;
using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;

namespace IMDBImport.Inserters
{
    public class PreparedInserter : IInserter
    {
        public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.Title_Genres (TConst, Genre) VALUES (@TConst, @Genre)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (Genre_Model genre in genres)
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
                    Console.WriteLine($"Error inserting genre: {ex.Message}");
                }
            }
        }

        public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.Names (NConst, PrimaryName, BirthYear, DeathYear) " +
                          "VALUES (@NConst, @PrimaryName, @BirthYear, @DeathYear)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (Name_Model name in names)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@NConst", name.NConst);
                sqlComm.Parameters.AddWithValue("@PrimaryName", name.PrimaryName);
                sqlComm.Parameters.AddWithValue("@BirthYear",
                    name.BirthYear.HasValue ? (object)name.BirthYear : DBNull.Value);
                sqlComm.Parameters.AddWithValue("@DeathYear",
                    name.DeathYear.HasValue ? (object)name.DeathYear : DBNull.Value);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting name: {ex.Message}");
                }
            }
        }

        public void InsertNameProfessions(List<NameProfession_Model> professions, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.Name_Professions (NConst, PrimaryProfession) " +
                          "VALUES (@NConst, @PrimaryProfession)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (NameProfession_Model profession in professions)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@NConst", profession.NConst);
                sqlComm.Parameters.AddWithValue("@PrimaryProfession", profession.PrimaryProfession);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting profession: {ex.Message}");
                }
            }
        }

        public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.Title (TConst, TitleType, PrimaryTitle, OriginalTitle, " +
                          "IsAdult, StartYear, EndYear, Runtime) " +
                          "VALUES (@TConst, @TitleType, @PrimaryTitle, @OriginalTitle, " +
                          "@IsAdult, @StartYear, @EndYear, @Runtime)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (Title_Model title in titles)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", title.TConst);
                sqlComm.Parameters.AddWithValue("@TitleType",
                    !string.IsNullOrEmpty(title.TitleType) ? (object)title.TitleType : DBNull.Value);
                sqlComm.Parameters.AddWithValue("@PrimaryTitle", title.PrimaryTitle);
                sqlComm.Parameters.AddWithValue("@OriginalTitle",
                    !string.IsNullOrEmpty(title.OriginalTitle) ? (object)title.OriginalTitle : DBNull.Value);
                sqlComm.Parameters.AddWithValue("@IsAdult", title.IsAdult);
                sqlComm.Parameters.AddWithValue("@StartYear",
                    title.StartYear.HasValue ? (object)title.StartYear : DBNull.Value);
                sqlComm.Parameters.AddWithValue("@EndYear",
                    title.EndYear.HasValue ? (object)title.EndYear : DBNull.Value);
                sqlComm.Parameters.AddWithValue("@Runtime",
                    title.Runtime.HasValue ? (object)title.Runtime : DBNull.Value);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting title: {ex.Message}");
                }
            }
        }

        public void InsertNameTitles(List<NameTitle_Model> nameTitleModels, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.Name_Titles (TConst, NConst, Category) " +
                          "VALUES (@TConst, @NConst, @Category)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (NameTitle_Model nameTitle in nameTitleModels)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", nameTitle.TConst);
                sqlComm.Parameters.AddWithValue("@NConst", nameTitle.NConst);
                sqlComm.Parameters.AddWithValue("@Category",
                    !string.IsNullOrEmpty(nameTitle.Category) ? (object)nameTitle.Category : DBNull.Value);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting name title: {ex.Message}");
                }
            }
        }

        public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.CrewDirectors (TConst, NConst) " +
                          "VALUES (@TConst, @NConst)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (CrewDirector_Model director in crewDirectors)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", director.TConst);
                sqlComm.Parameters.AddWithValue("@NConst", director.NConst);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting crew director: {ex.Message}");
                }
            }
        }

        public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
        {
            string query = "INSERT INTO dbo.CrewWriters (TConst, NConst) " +
                          "VALUES (@TConst, @NConst)";
            SqlCommand sqlComm = new SqlCommand(query, sqlConn);
            sqlComm.CommandTimeout = 300;
            sqlComm.Prepare();

            foreach (CrewWriter_Model writer in crewWriters)
            {
                sqlComm.Parameters.Clear();
                sqlComm.Parameters.AddWithValue("@TConst", writer.TConst);
                sqlComm.Parameters.AddWithValue("@NConst", writer.NConst);

                try
                {
                    sqlComm.ExecuteNonQuery();
                }
                catch (SqlException ex)
                {
                    Console.WriteLine($"Error inserting crew writer: {ex.Message}");
                }
            }
        }
    }
}