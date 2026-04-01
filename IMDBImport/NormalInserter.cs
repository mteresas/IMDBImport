using IMDBImport.Models;
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport
{
	public class NormalInserter : IInserter
	{
        public void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        public void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn) { throw new NotImplementedException(); }

        public void InsertNameProfessions(List<NameProfession_Model> nameProfessions, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        public void InsertNames(List<Name_Model> names, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        public void InsertNameTitles(List<NameTitle_Model> nameTitles, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        public void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn)
		{
			foreach (Title_Model movie in titles)
			{
				string query = $"INSERT INTO Titles (" +
								"TConst, " +
								"TitleType, " +
								"PrimaryTitle, " +
								"OriginalTitle, " +
								"IsAdult, " +
								"StartYear, " +
								"EndYear, " +
								"Runtime) " +
									$"VALUES (" +
									movie.TConst + ", " +
									"'" + movie.TitleType + "', " +
									"'" + movie.PrimaryTitle.Replace("'", "''") + "', " +
									"'" + movie.OriginalTitle.Replace("'", "''") + "', " +
									"'" + movie.IsAdult + "', " +
									ConvertIntToString(movie.StartYear) + ", " +
									ConvertIntToString(movie.EndYear) + ", " +
									ConvertIntToString(movie.Runtime) + ")";
				try
				{
					SqlCommand sqlComm = new SqlCommand(query, sqlConn);
					sqlComm.ExecuteNonQuery();
				}
				catch (SqlException sqlex)
				{
					Console.WriteLine("Error inserting new query: \r\n" + query);
				}
			}
		}

        public void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn)
        {
            throw new NotImplementedException();
        }

        private string ConvertIntToString(int? value)
		{
			return value.HasValue ? value.Value.ToString() : "NULL";
		}

	}
}
