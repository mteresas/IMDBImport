namespace IMDBImport.Models
{
    public class NameTitle_Model
    {
        public string TConst { get; set; }
        public string NConst { get; set; }
        public string Category { get; set; }

      
        public NameTitle_Model(string[] parts)
        {
            if (parts.Length < 4)
                throw new ArgumentException("Format invalide pour NameTitle");

            TConst = parts[0]?.Trim() ?? throw new ArgumentException("TConst ne peut pas être vide");
            NConst = parts[2]?.Trim() ?? throw new ArgumentException("NConst ne peut pas être vide");
            Category = parts[3]?.Trim() ?? throw new ArgumentException("Category ne peut pas être vide");
        }

       
        public NameTitle_Model()
        {
        }
    }
}