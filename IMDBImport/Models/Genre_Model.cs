using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IMDBImport.Models
{
	public class Genre_Model
	{
		public int TConst { get; set; }
		public string Genre { get; set; }

		public Genre_Model(string[] genreInfo)
		{
			TConst = int.Parse(genreInfo[0].Substring(2));
			Genre = genreInfo[1];
		}

		public override string ToString()
		{
			return TConst + " - " + Genre;
		}
	}
}
