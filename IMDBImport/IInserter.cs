using IMDBImport.Models;
using Microsoft.Data.SqlClient;

namespace IMDBImport
{
	public interface IInserter
	{
        void InsertTitles(List<Title_Model> titles, SqlConnection sqlConn);
        void InsertGenres(List<Genre_Model> genres, SqlConnection sqlConn);
        void InsertNames(List<Name_Model> names, SqlConnection sqlConn);
        void InsertNameProfessions(List<NameProfession_Model> nameProfessions, SqlConnection sqlConn);
        void InsertNameTitles(List<NameTitle_Model> nameTitles, SqlConnection sqlConn);
        void InsertCrewDirectors(List<CrewDirector_Model> crewDirectors, SqlConnection sqlConn);
        void InsertCrewWriters(List<CrewWriter_Model> crewWriters, SqlConnection sqlConn);

    }
}