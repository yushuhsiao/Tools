// https://gist.githubusercontent.com/kalebpederson/5460509/raw/10ac0a9f692158831d11c4badbbaadbc9536b3ef/ColumnAttributeTypeMapper.cs

using Dapper;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

namespace Dapper
{
	[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
	public class ColumnAttribute : Attribute
	{
		public string Name { get; set; }

		public ColumnAttribute(string name)
		{
			this.Name = name;
		}
	}

	/// <summary>
	/// Uses the Name value of the <see cref="ColumnAttribute"/> specified to determine
	/// the association between the name of the column in the query results and the member to
	/// which it will be extracted. If no column mapping is present all members are mapped as
	/// usual.
	/// </summary>
	/// <typeparam name="T">The type of the object that this association between the mapper applies to.</typeparam>
	public class ColumnAttributeTypeMapper<T> : SqlMapper.ITypeMap
	{
		private readonly IEnumerable<SqlMapper.ITypeMap> _mappers;

        private static PropertyInfo propertySelector(Type type, string columnName)
        {
            return type.GetProperties().FirstOrDefault(prop =>
            prop.GetCustomAttributes(false)
            .OfType<ColumnAttribute>()
            .Any(attr => attr.Name == columnName)
            );
        }


        public ColumnAttributeTypeMapper()
		{
            _mappers = new SqlMapper.ITypeMap[]
            {
                new CustomPropertyTypeMap(typeof(T), propertySelector),
                new DefaultTypeMap(typeof(T))
            };
		}

        ConstructorInfo SqlMapper.ITypeMap.FindConstructor(string[] names, Type[] types)
        {
            foreach (var mapper in _mappers)
            {
                try
                {
                    ConstructorInfo result = mapper.FindConstructor(names, types);
                    if (result != null)
                    {
                        return result;
                    }
                }
                catch (NotImplementedException)
                {
                }
            }
            return null;
        }

        ConstructorInfo SqlMapper.ITypeMap.FindExplicitConstructor()
        {
            return _mappers
                .Select(mapper => mapper.FindExplicitConstructor())
                .FirstOrDefault(result => result != null);
        }

        SqlMapper.IMemberMap SqlMapper.ITypeMap.GetConstructorParameter(ConstructorInfo constructor, string columnName)
        {
            foreach (var mapper in _mappers)
            {
                try
                {
                    var result = mapper.GetConstructorParameter(constructor, columnName);
                    if (result != null)
                    {
                        return result;
                    }
                }
                catch (NotImplementedException)
                {
                }
            }
            return null;
        }

        SqlMapper.IMemberMap SqlMapper.ITypeMap.GetMember(string columnName)
        {
            foreach (var mapper in _mappers)
            {
                try
                {
                    var result = mapper.GetMember(columnName);
                    if (result != null)
                    {
                        return result;
                    }
                }
                catch (NotImplementedException)
                {
                }
            }
            return null;
        }
    }
}