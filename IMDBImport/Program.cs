using System;
using System.Reflection.Metadata;

class Program
{
    static void Main()
    {
        Console.WriteLine("IMDB Interface");
        Console.WriteLine("1. Import data");
        Console.WriteLine("2. Launch application");
        Console.WriteLine("3. Exit");

        string choice = Console.ReadLine();

        switch (choice)
        {
            case "1":
                ImportData importer = new ImportData();
                importer.RunImport();
                break;

            case "2":
                InterfaceUI ui = new InterfaceUI();
                ui.Run();
                break;

            case "3":
                return;
        }
    }
}