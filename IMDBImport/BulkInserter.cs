using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;

namespace IMDBImport
{
    public class BulkInserter : IInserter
    {
        public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn)
        {
            DataTable genreTable = new DataTable();
            genreTable.Columns.Add("TConst", typeof(int));
            genreTable.Columns.Add("Genre", typeof(string));

            foreach (Genre_Model genre in genres)
            {
                genreTable.Rows.Add(
                    genre.TConst,
                    genre.Genre
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "Title_Genres";
                bulkCopy.WriteToServer(genreTable);
            }
        }

        public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
        {
            DataTable titleTable = new DataTable();
            titleTable.Columns.Add("TConst", typeof(int));
            titleTable.Columns.Add("TitleType", typeof(string));
            titleTable.Columns.Add("PrimaryTitle", typeof(string));
            titleTable.Columns.Add("OriginalTitle", typeof(string));
            titleTable.Columns.Add("IsAdult", typeof(bool));
            titleTable.Columns.Add("StartYear", typeof(int));
            titleTable.Columns.Add("EndYear", typeof(int));
            titleTable.Columns.Add("Runtime", typeof(int));

            foreach (Title_Model title in titles)
            {
                titleTable.Rows.Add(
                    title.TConst,
                    title.TitleType,
                    title.PrimaryTitle,
                    title.OriginalTitle,
                    title.IsAdult,
                    title.StartYear ?? (object)DBNull.Value,
                    title.EndYear ?? (object)DBNull.Value,
                    title.Runtime ?? (object)DBNull.Value
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "Title";
                bulkCopy.WriteToServer(titleTable);
            }
        }

        public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
        {
            DataTable crewDirectorsTable = new DataTable();
            crewDirectorsTable.Columns.Add("TConst", typeof(int));
            crewDirectorsTable.Columns.Add("NConst", typeof(int));

            foreach (CrewDirector_Model director in crewDirectors)
            {
                crewDirectorsTable.Rows.Add(
                    director.TConst,
                    director.NConst
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "CrewDirectors";
                bulkCopy.WriteToServer(crewDirectorsTable);
            }
        }

        public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
        {
            DataTable crewWritersTable = new DataTable();
            crewWritersTable.Columns.Add("TConst", typeof(int));
            crewWritersTable.Columns.Add("NConst", typeof(int));

            foreach (CrewWriter_Model writer in crewWriters)
            {
                crewWritersTable.Rows.Add(
                    writer.TConst,
                    writer.NConst
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "CrewWriters";
                bulkCopy.WriteToServer(crewWritersTable);
            }
        }

        public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
        {
            DataTable namesTable = new DataTable();
            namesTable.Columns.Add("NConst", typeof(int));
            namesTable.Columns.Add("PrimaryName", typeof(string));
            namesTable.Columns.Add("BirthYear", typeof(int));
            namesTable.Columns.Add("DeathYear", typeof(int));

            foreach (Name_Model name in names)
            {
                namesTable.Rows.Add(
                    name.NConst,
                    name.PrimaryName,
                    name.BirthYear ?? (object)DBNull.Value,
                    name.DeathYear ?? (object)DBNull.Value
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "Names";
                bulkCopy.WriteToServer(namesTable);
            }
        }

        public void InsertNameTitles(List<NameTitle_Model> nameTitles, SqlConnection sqlConn)
        {
            DataTable nameTitlesTable = new DataTable();
            nameTitlesTable.Columns.Add("NConst", typeof(int));
            nameTitlesTable.Columns.Add("TConst", typeof(int));

            foreach (NameTitle_Model nameTitle in nameTitles)
            {
                nameTitlesTable.Rows.Add(
                    nameTitle.NConst,
                    nameTitle.TConst
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "Name_Titles";
                bulkCopy.WriteToServer(nameTitlesTable);
            }
        }

        public void InsertNameProfessions(List<NameProfession_Model> nameProfessions, SqlConnection sqlConn)
        {
            DataTable nameProfessionsTable = new DataTable();
            nameProfessionsTable.Columns.Add("NConst", typeof(int));
            nameProfessionsTable.Columns.Add("PrimaryProfession", typeof(string));

            foreach (NameProfession_Model profession in nameProfessions)
            {
                nameProfessionsTable.Rows.Add(
                    profession.NConst,
                    profession.PrimaryProfession
                );
            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(sqlConn))
            {
                bulkCopy.DestinationTableName = "Name_Professions";
                bulkCopy.WriteToServer(nameProfessionsTable);
            }
        }
    }
}