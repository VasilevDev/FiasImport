using System;
using System.Xml;
using System.Collections.Generic;
using Npgsql;
using SharpCompress.Archives;
using System.Diagnostics;
using System.IO;
using System.Linq;

namespace FiasImport
{
	class Program
	{
		/// <summary>
		/// Основной метод выполнения импорта
		/// </summary>
		/// <param name="args"> Список аргументов
		/// args[0] - Путь к файлу архива с XML файлами данных по ФИАС
		/// args[1] - Строка подключения к БД
		/// args[2] - Строка со списком названий таблиц перечисленных через запятую
		/// </param>
		static void Main(string[] args)
		{
			try
			{
				if (args == null || args.Length != 3)
					throw new ArgumentException("Количество параметров не соответствует ожидаемому. " +
						"Ожидалось три параметра: 1- Путь к архиву справочника, 2 - Строка подключения к БД, " +
						"3 - Список названий таблиц, в которые необходимо импортировать данные");

				// Путь до архива с файлами данных по ФИАС
				string fiasArchivePath = args[0];

				// Проверим, что файл архива существует
				if (!File.Exists(fiasArchivePath))
					throw new FileNotFoundException($"Не удалось найти архив с файлами данных ФИАС по адресу {fiasArchivePath}");

				// Получаем список таблиц в которые будем производить импорт данных
				List<string> tables = args[2]?.Replace(" ", "")?.Split(',')?.ToList();

				if (tables == null)
					throw new ArgumentException("Не удалось получить список таблиц для импорта. Убедитесь, что верно заданны параметры приложения");

				if (!tables.Any())
					throw new ArgumentNullException("Список таблиц для импорта данных - пуст");

				using (var connection = new NpgsqlConnection(args[1]))
				using (var command = new NpgsqlCommand())
				{
					connection.Open();
					command.Connection = connection;

					// Открываем архив с файлами данных по ФИАС
					using (var archive = ArchiveFactory.Open(fiasArchivePath))
					{
						// Цикл обхода архива с XML файлами с данными ФИАС
						foreach (var entry in archive.Entries)
						{
							// Название таблицы полученное из название файла
							// Например файл называется AS_ADDROBJ_20171217_33bb6037-d55f-49e1-bb44-b24e834a7ff5.XML
							// из него получаем ADDROBJ
							string tableName = entry.Key.Split('_')[1];

							// Импортируем только нужные таблицы
							if (!tables.Contains(tableName)) continue;

							// Открываем поток на чтение XML файла ФИАС
							using (var stream = entry.OpenEntryStream())
							{
								XmlReader reader = XmlReader.Create(stream);

								Console.WriteLine($"Запущен процесс импорта таблицы {tableName}");

								var timer = new Stopwatch();
								timer.Start();
								int recordCount = 0;

								// Цикл обхода XML файла ФИАС
								while (reader.Read())
								{
									if (reader.NodeType == XmlNodeType.Element)
									{
										//Атрибуты и их значения представляют собой название поля в таблице и значение соответственно
										if (reader.HasAttributes)
										{
											var record = new Dictionary<string, object>();

											//Цикл обхода всех полей записи таблицы
											while (reader.MoveToNextAttribute())
											{
												string fieldName = reader.Name.ToLower();
												string fieldValue = $"'{reader.Value.Replace("'", "").Replace('«', '"').Replace('»', '"')}'";

												record[fieldName] = fieldValue;
											}

											var actstatus = record["actstatus"].ToString().Replace("'", ""); // статус актуальности ФИАС
											var currstatus = record["currstatus"].ToString().Replace("'", ""); // статус актуальности КЛАДР

											//Пишем в БД только актуальные адресные объекты, actstatus == 1 и currstatus == 0
											if (tableName.ToLower() == "addrobj" && int.Parse(actstatus) == 1 && int.Parse(currstatus) == 0)
											{
												//Формируем insert запрос
												command.CommandText = FillInsertCommand(tableName.ToLower(), record);
												command.ExecuteNonQuery();
												recordCount++;
											}
										}
									}
								}

								timer.Stop();

								Console.WriteLine($"Импорт данных таблицы {tableName} выполнен успешно. Время импорта составило: {timer.Elapsed}. Импортировано {recordCount} записей.");

								// Если это табличка адресов, то рассчитаем полный адрес для нужных записей
								if (tableName.ToLower() == "addrobj")
								{
									var t = new Stopwatch();

									try
									{
										Console.WriteLine($"Выполняем рассчет полного адреса для каждого адресного объекта");
										t.Start();
										int updRowCount = CalculateAndUpdateFullAddress(args[1]);
										t.Stop();
										Console.WriteLine($"Рассчитали полный адрес для {updRowCount} записей. С момента начала вычисления прошло: {t.Elapsed}.");
									}
									catch(Exception ex)
									{
										t.Stop();
										throw new Exception($"Ошибка при вычислении полных адресов. Время, прошедшее с момента запуска: {t.Elapsed}. Ошибка: {ex.Message}.");
									}
								}
							}
						}
					}
				}

				Console.WriteLine("Импорт базы ФИАС выполнен успешно!");
			}
			catch (Exception ex)
			{
				Console.ForegroundColor = ConsoleColor.Red;
				Console.WriteLine($"Критическая ошибка при выполнени импорта данных по ФИАС. Процесс импорта полностью остановлен. {ex.Message}.");
				Console.ForegroundColor = ConsoleColor.White;
			}

			Console.ReadLine();
		}

		/// <summary>
		/// Метод формирования строки добавления записи фиас в таблицу
		/// </summary>
		/// <param name="tableName"></param>
		/// <param name="data"></param>
		/// <returns></returns>
		private static string FillInsertCommand(string tableName, Dictionary<string, object> data)
		{
			string sql = @"INSERT INTO public.{0} ({1}) VALUES ({2})";

			// Дата создания записи
			var createDate = $"'{DateTime.UtcNow.ToString("o")}'";

			data["recid"] = $"'{Guid.NewGuid()}'";
			data["reccreated"] = createDate;
			data["recupdated"] = createDate;
			data["recstate"] = 1;

			sql = string.Format(sql, "rdev___fias_addrobj",
				string.Join(',', data.Keys).ToLower(),
				string.Join(',', data.Values)
			);

			return sql;
		}
	
		/// <summary>
		/// Рассчет полных адресов ФИАС
		/// </summary>
		/// <param name="connectionString"></param>
		/// <returns>Количество обновленных записей</returns>
		private static int CalculateAndUpdateFullAddress(string connectionString)
		{
			var connection = new NpgsqlConnection(connectionString);

			try
			{
				using (var command = new NpgsqlCommand("UPDATE rdev___fias_addrobj ao SET fulladdress = get_addrobj_fulladdress(ao.aoguid) WHERE ao.recstate = 1"))
				{
					connection.Open();
					command.Connection = connection;

					int count = command.ExecuteNonQuery();
					return count;
				}
			}
			finally
			{
				connection.Close();
			}
		}
	}
}
