using System;
using System.Collections.Generic;
using System.Data;
using IMDBImport;
using IMDBImport.Models;
using Microsoft.Data.SqlClient;

namespace IMDBImport.Inserters
{
    public class BulkInserter : IInserter
    {
        private const int BATCH_SIZE = 10000;
        private const int COMMAND_TIMEOUT = 600; // 10 minutes

        public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
        {
            if (genres == null || genres.Count == 0)
                return;

            Console.WriteLine($"Inserting {genres.Count:N0} genres...");

            try
            {
                using (DataTable dt = ConvertGenresToDataTable(genres))
                {
                    BulkInsert(sqlConn, dt, "dbo.Title_Genres");
                }
                Console.WriteLine("✓ Genres inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting genres: {ex.Message}");
            }
        }

        public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
        {
            if (names == null || names.Count == 0)
                return;

            Console.WriteLine($"Inserting {names.Count:N0} names...");

            try
            {
                using (DataTable dt = ConvertNamesToDataTable(names))
                {
                    BulkInsert(sqlConn, dt, "dbo.Names");
                }
                Console.WriteLine("✓ Names inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting names: {ex.Message}");
            }
        }

        public void InsertNameProfessions(List<NameProfession_Model> professions, SqlConnection sqlConn)
        {
            if (professions == null || professions.Count == 0)
                return;

            Console.WriteLine($"Inserting {professions.Count:N0} professions...");

            try
            {
                using (DataTable dt = ConvertNameProfessionsToDataTable(professions))
                {
                    BulkInsert(sqlConn, dt, "dbo.Name_Professions");
                }
                Console.WriteLine("✓ Professions inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting professions: {ex.Message}");
            }
        }

        public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
        {
            if (titles == null || titles.Count == 0)
                return;

            Console.WriteLine($"Inserting {titles.Count:N0} titles...");

            try
            {
                using (DataTable dt = ConvertTitlesToDataTable(titles))
                {
                    BulkInsert(sqlConn, dt, "dbo.Title");
                }
                Console.WriteLine("✓ Titles inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting titles: {ex.Message}");
            }
        }

        public void InsertNameTitles(List<NameTitle_Model> nameTitles, SqlConnection sqlConn)
        {
            if (nameTitles == null || nameTitles.Count == 0)
                return;

            Console.WriteLine($"Inserting {nameTitles.Count:N0} name titles...");

            try
            {
                using (DataTable dt = ConvertNameTitleToDataTable(nameTitles))
                {
                    BulkInsert(sqlConn, dt, "dbo.Name_Titles");
                }
                Console.WriteLine("✓ Name titles inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting name titles: {ex.Message}");
            }
        }

        public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
        {
            if (crewDirectors == null || crewDirectors.Count == 0)
                return;

            Console.WriteLine($"Inserting {crewDirectors.Count:N0} crew directors...");

            try
            {
                using (DataTable dt = ConvertCrewDirectorsToDataTable(crewDirectors))
                {
                    BulkInsert(sqlConn, dt, "dbo.CrewDirectors");
                }
                Console.WriteLine("✓ Crew directors inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting crew directors: {ex.Message}");
            }
        }

        public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
        {
            if (crewWriters == null || crewWriters.Count == 0)
                return;

            Console.WriteLine($"Inserting {crewWriters.Count:N0} crew writers...");

            try
            {
                using (DataTable dt = ConvertCrewWritersToDataTable(crewWriters))
                {
                    BulkInsert(sqlConn, dt, "dbo.CrewWriters");
                }
                Console.WriteLine("✓ Crew writers inserted");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting crew writers: {ex.Message}");
            }
        }

        // ===== DataTable Conversion Methods =====

        private DataTable ConvertTitlesToDataTable(List<Title_Model> titles)
        {
            DataTable dt = new DataTable("Title");
            dt.Columns.Add("TConst", typeof(string));
            dt.Columns.Add("TitleType", typeof(string));
            dt.Columns.Add("PrimaryTitle", typeof(string));
            dt.Columns.Add("OriginalTitle", typeof(string));
            dt.Columns.Add("IsAdult", typeof(bool));
            dt.Columns.Add("StartYear", typeof(int));
            dt.Columns.Add("EndYear", typeof(int));
            dt.Columns.Add("Runtime", typeof(int));

            foreach (var title in titles)
            {
                dt.Rows.Add(
                    title.TConst,
                    title.TitleType ?? (object)DBNull.Value,
                    title.PrimaryTitle,
                    title.OriginalTitle ?? (object)DBNull.Value,
                    title.IsAdult,
                    title.StartYear ?? (object)DBNull.Value,
                    title.EndYear ?? (object)DBNull.Value,
                    title.Runtime ?? (object)DBNull.Value
                );
            }

            return dt;
        }

        private DataTable ConvertGenresToDataTable(List<Genre_Model> genres)
        {
            DataTable dt = new DataTable("Title_Genres");
            dt.Columns.Add("TConst", typeof(string));
            dt.Columns.Add("Genre", typeof(string));

            foreach (var genre in genres)
            {
                dt.Rows.Add(genre.TConst, genre.Genre);
            }

            return dt;
        }

        private DataTable ConvertNamesToDataTable(List<Name_Model> names)
        {
            DataTable dt = new DataTable("Names");
            dt.Columns.Add("NConst", typeof(string));
            dt.Columns.Add("PrimaryName", typeof(string));
            dt.Columns.Add("BirthYear", typeof(int));
            dt.Columns.Add("DeathYear", typeof(int));

            foreach (var name in names)
            {
                dt.Rows.Add(
                    name.NConst,
                    name.PrimaryName,
                    name.BirthYear ?? (object)DBNull.Value,
                    name.DeathYear ?? (object)DBNull.Value
                );
            }

            return dt;
        }

        private DataTable ConvertNameProfessionsToDataTable(List<NameProfession_Model> professions)
        {
            DataTable dt = new DataTable("Name_Professions");
            dt.Columns.Add("NConst", typeof(string));
            dt.Columns.Add("PrimaryProfession", typeof(string));

            foreach (var profession in professions)
            {
                dt.Rows.Add(profession.NConst, profession.PrimaryProfession);
            }

            return dt;
        }

        private DataTable ConvertCrewDirectorsToDataTable(List<CrewDirector_Model> directors)
        {
            DataTable dt = new DataTable("CrewDirectors");
            dt.Columns.Add("TConst", typeof(string));
            dt.Columns.Add("NConst", typeof(string));

            foreach (var director in directors)
            {
                dt.Rows.Add(director.TConst, director.NConst);
            }

            return dt;
        }

        private DataTable ConvertCrewWritersToDataTable(List<CrewWriter_Model> writers)
        {
            DataTable dt = new DataTable("CrewWriters");
            dt.Columns.Add("TConst", typeof(string));
            dt.Columns.Add("NConst", typeof(string));

            foreach (var writer in writers)
            {
                dt.Rows.Add(writer.TConst, writer.NConst);
            }

            return dt;
        }

        private DataTable ConvertNameTitleToDataTable(List<NameTitle_Model> nameTitles)
        {
            DataTable dt = new DataTable("Name_Titles");
            dt.Columns.Add("TConst", typeof(string));
            dt.Columns.Add("NConst", typeof(string));
            dt.Columns.Add("Category", typeof(string));

            foreach (var nameTitle in nameTitles)
            {
                dt.Rows.Add(
                    nameTitle.TConst,
                    nameTitle.NConst,
                    nameTitle.Category ?? (object)DBNull.Value
                );
            }

            return dt;
        }

        // ===== Bulk Insert Generic Method =====

        private void BulkInsert(SqlConnection connection, DataTable dataTable, string destinationTable)
        {
            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(connection))
            {
                bulkCopy.DestinationTableName = destinationTable;
                bulkCopy.BatchSize = BATCH_SIZE;
                bulkCopy.BulkCopyTimeout = COMMAND_TIMEOUT;
                bulkCopy.EnableStreaming = true;

                // Map columns
                foreach (DataColumn column in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.WriteToServer(dataTable);
            }
        }
    }
}