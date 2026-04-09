using System;

public class InterfaceUI
{
    private DBSecurity db = new DBSecurity();

    public void Run()
    {
        while (true)
        {
            Console.WriteLine("\nIMDB MENU");
            Console.WriteLine("1.1) Search Movie");
            Console.WriteLine("1.2) Add Movie");
            Console.WriteLine("1.3) Update Movie");
            Console.WriteLine("1.4) Delete Movie");
            Console.WriteLine("2.1) Search Person");
            Console.WriteLine("2.2) Add Person");
            Console.WriteLine("3) Exit");

            string choice = Console.ReadLine() ?? "";

            try
            {
                switch (choice)
                {
                    case "1.1": SearchMovie(); break;
                    case "1.2": AddMovie(); break;
                    case "1.3": UpdateMovie(); break;
                    case "1.4": DeleteMovie(); break;
                    case "2.1": SearchPerson(); break;
                    case "2.2": AddPerson(); break;
                    case "3": return;
                    default: Console.WriteLine("Invalid choice"); break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }

    private void SearchMovie()
    {
        Console.Write("Enter movie title: ");
        string input = Console.ReadLine() ?? "";
        db.SearchMovies(input, true);
    }

    private void SearchPerson()
    {
        Console.Write("Enter person name: ");
        string input = Console.ReadLine() ?? "";
        db.SearchPersons(input, true);
    }

    private void AddMovie()
    {
        Console.Write("Title: ");
        string title = Console.ReadLine() ?? "";

        Console.Write("Year: ");
        if (!int.TryParse(Console.ReadLine(), out int year))
        {
            Console.WriteLine("Invalid year!");
            return;
        }

        db.AddMovie(title, year);
    }

    private void AddPerson()
    {
        Console.Write("Name: ");
        string name = Console.ReadLine() ?? "";

        Console.Write("Birth Year: ");
        if (!int.TryParse(Console.ReadLine(), out int year))
        {
            Console.WriteLine("Invalid year!");
            return;
        }

        db.AddPerson(name, year);
    }

    private void UpdateMovie()
    {
        Console.Write("ID (ttxxxxxxx): ");
        string id = Console.ReadLine() ?? "";

        Console.Write("New Title: ");
        string title = Console.ReadLine() ?? "";

        Console.Write("New Year: ");
        if (!int.TryParse(Console.ReadLine(), out int year))
        {
            Console.WriteLine("Invalid year!");
            return;
        }

        db.UpdateMovie(id, title, year);
    }

    private void DeleteMovie()
    {
        Console.Write("ID : ");
        string id = Console.ReadLine() ?? "";

        db.DeleteMovie(id);
    }
}