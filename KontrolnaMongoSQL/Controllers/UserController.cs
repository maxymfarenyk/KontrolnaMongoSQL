using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using MongoDB.Bson;
using MongoDB.Driver;
using MySql.Data.MySqlClient;
using Newtonsoft.Json;
using KontrolnaMongoSQL.Models;

namespace KontrolnaMongoSQL.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public class UserController : ControllerBase
    {
        private readonly MongoClient _mongoClient;
        private readonly IMongoCollection<BsonDocument> _collection;
        private readonly MySqlConnection _mySqlConnection;

        public UserController()
        {
            // підключення до MySQL
            _mySqlConnection = new MySqlConnection("Server=localhost;Database=kontrolnadb;Uid=root;Pwd=4815162342;");
            _mySqlConnection.Open();
        }

        [HttpGet]
        public IActionResult GetAllUsers()
        {
            var mongoUsers = new List<UserCompany>();

            // підключаємось до бази даних MongoDB
            var client = new MongoClient("mongodb://localhost:27017");
            var database = client.GetDatabase("kontrolna_robota");

            // зчитуємо дані з колекції MongoUser та додаємо їх до масиву users
            var collection = database.GetCollection<BsonDocument>("MongoUser");
            var filter = new BsonDocument();
            var cursor = collection.Find(filter);
            
            foreach (var document in cursor.ToEnumerable())
            {
                var id = document.GetValue("id").ToString();
                var name = document.GetValue("name").ToString();
                var age = document["Personal_data"]["age"].AsInt32;
                var homeAddress = document["Personal_data"]["home_address"].ToString();
                var companyName = document["Work_data"]["company_name"].ToString();
                var address = document["Work_data"]["address"].ToString();
                var experience = document["Work_data"]["experience"].ToString();

                var userCompany = new UserCompany
                {
                    Id = id,
                    Name = name,
                    PersonalData = new PersonalData
                    {
                        Age = age,
                        HomeAddress = homeAddress
                    },
                    WorkData = new WorkData
                    {
                        CompanyName = companyName,
                        Address = address,
                        Experience = experience
                    }
                };

                mongoUsers.Add(userCompany);
            }

            // вибрати всі записи з таблиць mysql і об'єднати їх
            var mysqlUsers = new List<UserCompany>();
            var command = new MySqlCommand("SELECT * FROM kontr_user JOIN kontr_company ON kontr_user.id = kontr_company.user_id", _mySqlConnection);
            var reader = command.ExecuteReader();


            while (reader.Read())
            {
                var id = reader.GetString("id");
                var name = reader.GetString("name");
                var age = reader.GetInt32("age");
                var homeAddress = reader.GetString("home_address");
                var companyName = reader.GetString("company_name");
                var address = reader.GetString("address");
                var experience = reader.GetString("experience");

                var userCompany = new UserCompany
                {
                    Id = id,
                    Name = name,
                    PersonalData = new PersonalData
                    {
                        Age = age,
                        HomeAddress = homeAddress
                    },
                    WorkData = new WorkData
                    {
                        CompanyName = companyName,
                        Address = address,
                        Experience = experience
                    }
                };

                mysqlUsers.Add(userCompany);
            }

            reader.Close();

            // Об'єднання даних
            var allUsers = mongoUsers.Concat(mysqlUsers).ToList();

            // Вставка даних в нову таблицю MySQL
            var insertCommand = new MySqlCommand("INSERT INTO NewUsersSQL (id, name, age, home_address, company_name, address, experience) VALUES (@id, @name, @age, @homeAddress, @companyName, @address, @experience)", _mySqlConnection);
            insertCommand.Parameters.Add("@id", MySqlDbType.VarChar);
            insertCommand.Parameters.Add("@name", MySqlDbType.VarChar);
            insertCommand.Parameters.Add("@age", MySqlDbType.Int32);
            insertCommand.Parameters.Add("@homeAddress", MySqlDbType.VarChar);
            insertCommand.Parameters.Add("@companyName", MySqlDbType.VarChar);
            insertCommand.Parameters.Add("@address", MySqlDbType.VarChar);
            insertCommand.Parameters.Add("@experience", MySqlDbType.VarChar);

            foreach (var user in allUsers)
            {
                insertCommand.Parameters["@id"].Value = user.Id;
                insertCommand.Parameters["@name"].Value = user.Name;
                insertCommand.Parameters["@age"].Value = user.PersonalData.Age;
                insertCommand.Parameters["@homeAddress"].Value = user.PersonalData.HomeAddress;
                insertCommand.Parameters["@companyName"].Value = user.WorkData.CompanyName;
                insertCommand.Parameters["@address"].Value = user.WorkData.Address;
                insertCommand.Parameters["@experience"].Value = user.WorkData.Experience;

                insertCommand.ExecuteNonQuery();
            }
            _mySqlConnection.Close();
            // Повернення результату
            var json = JsonConvert.SerializeObject(allUsers, Formatting.Indented);
            return Ok(json);
        }
    }
}