using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;


namespace DeepCopy
{
    public static class Mapper
    {
        private static readonly ConcurrentDictionary<DictionaryKey<Type, Type, bool>, Lazy<Action<object, object>>> cachedActions = new ConcurrentDictionary<DictionaryKey<Type, Type, bool>, Lazy<Action<object, object>>>();
        private static readonly ConcurrentDictionary<DictionaryKey<Type, Type, bool>, Lazy<Func<object, object>>> cachedFunctions = new ConcurrentDictionary<DictionaryKey<Type, Type, bool>, Lazy<Func<object, object>>>();
        private static readonly ConcurrentDictionary<DictionaryKey<Type, Type>, Lazy<Type>> assumeDestinationActualTypesCache = new ConcurrentDictionary<DictionaryKey<Type, Type>, Lazy<Type>>();

        public static TDestination DeepCopyTo<TDestination>(this object source, bool assumeDestinationExplicitly = true)
        {
            if (source == null)
                return default(TDestination);

            Type sourceType = source.GetType();

            return (TDestination)MapTo(source, sourceType, typeof(TDestination), assumeDestinationExplicitly);
        }

        public static object DeepClone(this object source)
        {
            if (source == null)
                return null;

            Type sourceType = source.GetType();

            return MapTo(source, sourceType, sourceType, true);
        }

        public static void Map<TSource, TDestination>(TSource source, TDestination destination, bool assumeDestinationExplicitly = true)
            where TSource : class
            where TDestination : class
        {
            if (destination == default(TDestination))
                return;

            Map(source, typeof(TSource), destination, typeof(TDestination), assumeDestinationExplicitly);
        }

        private static object MapTo(object source, Type sourceType, Type destinationType, bool assumeDestinationExplicitly)
        {
            DictionaryKey<Type, Type, bool> key = new DictionaryKey<Type, Type, bool>(sourceType, destinationType, assumeDestinationExplicitly);

            Lazy<Func<object, object>> mappingFunction;

            if (!cachedFunctions.TryGetValue(key, out mappingFunction))
            {
                mappingFunction = CacheMappingFunction(key);
            }

            return mappingFunction.Value(source);
        }

        private static void Map(object source, Type sourceType, object destination, Type destinationType, bool assumeDestinationExplicitly)
        {
            DictionaryKey<Type, Type, bool> key = new DictionaryKey<Type, Type, bool>(sourceType, destinationType, assumeDestinationExplicitly);

            Lazy<Action<object, object>> mappingAction;

            if (!cachedActions.TryGetValue(key, out mappingAction))
            {
                mappingAction = CacheMappingAction(key);
            }

            mappingAction.Value(source, destination);
        }

        private static Lazy<Func<object, object>> CacheMappingFunction(DictionaryKey<Type, Type, bool> key)
        {
            return cachedFunctions.GetOrAdd(
                key,
                new Lazy<Func<object, object>>(() => GetMappingFunction(key.Item1, key.Item2, key.Item3), true));
        }

        private static Lazy<Action<object, object>> CacheMappingAction(DictionaryKey<Type, Type, bool> key)
        {
            return cachedActions.GetOrAdd(
                key,
                new Lazy<Action<object, object>>(() => GetMappingAction(key.Item1, key.Item2, key.Item3), true));
        }

        private static Func<object, object> GetMappingFunction(Type sourceType, Type destinationType, bool assumeDestinationExplicitly)
        {
            ParameterExpression sourceObj = Expression.Parameter(typeof(object), "sourceObject");
            ParameterExpression sourceVar = Expression.Variable(sourceType, "source");
            ParameterExpression destinationVar = Expression.Variable(destinationType, "destination");

            List<Expression> expressions = new List<Expression>(4);

            expressions.Add(Expression.Assign(sourceVar, Expression.Convert(sourceObj, sourceType)));

            if (!destinationType.IsValueType)
                expressions.Add(Expression.Assign(destinationVar, Expression.Constant(null, destinationType)));

            expressions.Add(ExpressionToMapObjects(sourceVar, sourceType, null, destinationVar, destinationType, null, false, destinationType, assumeDestinationExplicitly));

            if (destinationType.IsValueType)
                expressions.Add(Expression.Convert(destinationVar, typeof(object)));
            else
                expressions.Add(destinationVar);

            LambdaExpression mappingLambda = Expression.Lambda(Expression.Block(new ParameterExpression[] { sourceVar, destinationVar }, expressions),
                                                               new ParameterExpression[] { sourceObj });

            return (Func<object, object>)mappingLambda.Compile();
        }

        private static Action<object, object> GetMappingAction(Type sourceType, Type destinationType, bool assumeDestinationExplicitly)
        {
            ParameterExpression sourceObj = Expression.Parameter(typeof(object), "sourceObject");

            ParameterExpression sourceVar = Expression.Variable(sourceType, "source");
            BinaryExpression assignSourceVar = Expression.Assign(sourceVar, Expression.Convert(sourceObj, sourceType));

            ParameterExpression destinationObj = Expression.Parameter(typeof(object), "destinationObject");
            ParameterExpression destinationVar = Expression.Variable(destinationType, "destination");
            BinaryExpression assignDestinationVar = Expression.Assign(destinationVar, Expression.Convert(destinationObj, destinationType));

            Expression mappingExpression = ExpressionToMapObjects(sourceVar, sourceType, null, destinationVar, destinationType, null, true, destinationType, assumeDestinationExplicitly);

            LambdaExpression mappingLambda = Expression.Lambda(Expression.Block(new ParameterExpression[] { sourceVar, destinationVar },
                                                                                new Expression[] { assignSourceVar, assignDestinationVar, mappingExpression }),
                                                               new ParameterExpression[] { sourceObj, destinationObj });

            return (Action<object, object>)mappingLambda.Compile();
        }

        private static Expression ExpressionToMapObjects(Expression sourceTarget, Type sourceType, string sourceMemberName, Expression destinationTarget, Type destinationType, string destinationMemberName, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            Expression sourceExpr;
            if (string.IsNullOrEmpty(sourceMemberName))
                sourceExpr = sourceTarget;
            else
                sourceExpr = Expression.PropertyOrField(sourceTarget, sourceMemberName);

            Expression destinationExpr;
            if (string.IsNullOrEmpty(destinationMemberName))
                destinationExpr = destinationTarget;
            else
                destinationExpr = Expression.PropertyOrField(destinationTarget, destinationMemberName);

            if (sourceMemberName != null && CanDeepCopyPropertyOrField(sourceType, destinationType, assumeDestinationExplicitly))
            {
                return Expression.Assign(destinationExpr, Expression.Call(typeof(Mapper).GetMethod("DeepCopyTo", BindingFlags.Static | BindingFlags.Public).MakeGenericMethod(new[] { destinationType }), sourceExpr, Expression.Constant(assumeDestinationExplicitly)));
            }

            if (sourceType.IsValueType || destinationType.IsValueType)
                return ExpressionToMapValueTypes(sourceExpr, sourceType, destinationExpr, destinationType);

            if (sourceType == typeof(string))
            {
                if (destinationType == typeof(string))
                    return Expression.Assign(destinationExpr, sourceExpr);
                else if (destinationType == typeof(object))
                    return Expression.Assign(destinationExpr, Expression.Convert(sourceExpr, typeof(object)));

                return Expression.Empty();
            }

            if (sourceType == typeof(object))
            {
                if (destinationType == typeof(object))
                    return ExpressionToMapObjectToObject(sourceExpr, destinationExpr, initialDestinationType);

                return Expression.Empty();
            }

            Type sourceGenericType = sourceType.IsGenericType ? sourceType.GetGenericTypeDefinition() : null;
            Type destinationGenericType = destinationType.IsGenericType ? destinationType.GetGenericTypeDefinition() : null;

            if (sourceGenericType != null && sourceGenericType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
            {
                if (destinationGenericType != null && destinationGenericType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>)))
                    return ExpressionToMapDictionaries(sourceExpr, sourceType, destinationExpr, destinationType, mapExistingDestination, initialDestinationType, false);

                return Expression.Empty();
            }

            if (sourceType.IsArray || sourceGenericType == typeof(List<>))
            {
                if (destinationType.IsArray || destinationGenericType == typeof(List<>))
                    return ExpressionToMapCollections(sourceExpr, sourceType, destinationExpr, destinationType, mapExistingDestination, initialDestinationType, assumeDestinationExplicitly);
                return Expression.Empty();
            }

            if (sourceType.IsAbstract || destinationType.IsAbstract || (!assumeDestinationExplicitly && (HasInheritedTypes(destinationType))))
            {
                return ExpressionToMapAbstractTypes(sourceExpr, sourceType, destinationExpr, destinationType, mapExistingDestination, initialDestinationType, false);
            }

            List<Expression> expressions = new List<Expression>();

            expressions.Add(ExpressionToInstantiateDestination(destinationExpr, null, destinationType, mapExistingDestination));
            expressions.AddRange(ExpressionToMapFields(sourceExpr, sourceType, destinationExpr, destinationType, mapExistingDestination, initialDestinationType, false));
            expressions.AddRange(ExpressionToMapProperties(sourceExpr, sourceType, destinationExpr, destinationType, mapExistingDestination, initialDestinationType, false));

            BinaryExpression sourceNotNull = Expression.NotEqual(sourceExpr, Expression.Constant(null));
            return Expression.IfThen(sourceNotNull, Expression.Block(expressions));
        }

        private static Expression ExpressionToMapValueTypes(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType)
        {
            Type sourceGenericType = sourceType.IsGenericType ? sourceType.GetGenericTypeDefinition() : null;
            Type destinationGenericType = destinationType.IsGenericType ? destinationType.GetGenericTypeDefinition() : null;

            if (sourceGenericType == typeof(Nullable<>) || destinationGenericType == typeof(Nullable<>))
            {
                Type actualSourceType = null;
                Type actualDestinationType = null;
                Expression sourceValue = null;

                if (sourceGenericType == typeof(Nullable<>))
                {
                    actualSourceType = sourceType.GetGenericArguments()[0];
                    sourceValue = Expression.Property(sourceExpr, "Value");
                }
                else
                {
                    actualSourceType = sourceType;
                    sourceValue = sourceExpr;
                }

                if (destinationGenericType == typeof(Nullable<>))
                    actualDestinationType = destinationType.GetGenericArguments()[0];
                else
                    actualDestinationType = destinationType;

                Expression assignExpression = null;

                if (actualSourceType.IsEnum)
                {
                    if (actualDestinationType.IsEnum)
                    {
                        if (actualSourceType == destinationType)
                            assignExpression = Expression.Assign(destinationExpr, sourceValue);
                        else
                            assignExpression = Expression.Assign(destinationExpr, Expression.Convert(sourceValue, destinationType));
                    }
                    else
                    {
                        if (IsEnumUnderlyingType(actualDestinationType))
                            assignExpression = Expression.Assign(destinationExpr, Expression.Convert(sourceValue, destinationType));
                    }
                }
                else
                {
                    if (actualSourceType.IsValueType)
                    {
                        if (actualDestinationType.IsEnum && IsEnumUnderlyingType(actualSourceType))
                            assignExpression = Expression.Assign(destinationExpr, Expression.Convert(sourceValue, destinationType));

                        if (actualSourceType == actualDestinationType)
                        {
                            if (actualSourceType == destinationType)
                                assignExpression = Expression.Assign(destinationExpr, sourceValue);
                            else
                                assignExpression = Expression.Assign(destinationExpr, Expression.Convert(sourceValue, destinationType));
                        }
                    }
                }

                if (assignExpression == null)
                    return Expression.Empty();

                if (sourceGenericType == typeof(Nullable<>))
                    return Expression.IfThenElse(Expression.Property(sourceExpr, "HasValue"),
                                                    assignExpression,
                                                    Expression.Assign(destinationExpr, Expression.Default(destinationType)));

                return assignExpression;
            }

            if (sourceType.IsEnum)
            {
                if (destinationType.IsEnum)
                {
                    if (sourceType == destinationType)
                        return Expression.Assign(destinationExpr, sourceExpr);
                    return Expression.Assign(destinationExpr, Expression.Convert(sourceExpr, destinationType));
                }

                if (IsEnumUnderlyingType(destinationType) || destinationType == typeof(object))
                    return Expression.Assign(destinationExpr, Expression.Convert(sourceExpr, destinationType));

                return Expression.Empty();
            }

            if (sourceType.IsValueType)
            {
                if (destinationType.IsEnum && IsEnumUnderlyingType(sourceType))
                    return Expression.Assign(destinationExpr, Expression.Convert(sourceExpr, destinationType));

                if (sourceType == destinationType)
                    return Expression.Assign(destinationExpr, sourceExpr);
                else if (destinationType == typeof(object))
                    return Expression.Assign(destinationExpr, Expression.Convert(sourceExpr, typeof(object)));
            }

            return Expression.Empty();
        }

        private static bool IsEnumUnderlyingType(Type type)
        {
            return type.IsPrimitive && type != typeof(bool) && type != typeof(char) && type != typeof(double) && type != typeof(float);
        }

        private static Expression ExpressionToMapObjectToObject(Expression sourceExpr, Expression destinationExpr, Type initialDestinationType)
        {
            MethodInfo getType = typeof(object).GetMethod("GetType");
            MethodInfo assumeDestinationActualType = typeof(Mapper).GetMethod("AssumeDestinationActualType", BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo mapTo = typeof(Mapper).GetMethod("MapTo", BindingFlags.Static | BindingFlags.NonPublic);

            ParameterExpression sourceActualType = Expression.Variable(typeof(Type));
            ParameterExpression destinationActualType = Expression.Parameter(typeof(Type));

            Expression assignDestination = Expression.Block(
                new ParameterExpression[] { sourceActualType, destinationActualType },
                new Expression[]
                {
                    Expression.Assign(sourceActualType, Expression.Call(sourceExpr, getType)),
                    Expression.Assign(destinationActualType, Expression.Call(assumeDestinationActualType, new Expression[] { Expression.Constant(initialDestinationType), sourceActualType })),

                    Expression.IfThen(
                        Expression.And(
                            Expression.Equal(destinationActualType, Expression.Constant(null)),
                            Expression.Equal(Expression.Property(sourceActualType,"IsEnum"), Expression.Constant(true))),
                        Expression.Assign(destinationActualType, Expression.Constant(typeof(int)))),

                    Expression.IfThenElse(Expression.Equal(destinationActualType, Expression.Constant(null)),
                        Expression.Assign(destinationExpr, Expression.Constant(null)),
                        Expression.Assign(destinationExpr, Expression.Call(mapTo, new [] { sourceExpr, sourceActualType, destinationActualType, Expression.Constant(true) }))),
                });

            return Expression.IfThenElse(Expression.Equal(sourceExpr, Expression.Constant(null)),
                Expression.Assign(destinationExpr, Expression.Constant(null)),
                assignDestination);
        }

        private static Expression ExpressionToMapDictionaries(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            Type sourcePairType = sourceType.GetInterface("IEnumerable`1").GetGenericArguments()[0];
            Type sourceKeyType = sourceType.GetGenericArguments()[0];
            Type sourceValueType = sourceType.GetGenericArguments()[1];

            Type destinationKeyType = destinationType.GetGenericArguments()[0];
            Type destinationValueType = destinationType.GetGenericArguments()[1];

            MethodInfo getEnumerator = sourceType.GetMethod("GetEnumerator");
            Type enumeratorType = getEnumerator.ReturnType;
            ParameterExpression enumerator = Expression.Variable(enumeratorType);
            Expression assignEnumerator = Expression.Assign(enumerator, Expression.Call(sourceExpr, getEnumerator));
            LabelTarget breakLabel = Expression.Label();
            ConditionalExpression breakLoop = Expression.IfThen(Expression.IsFalse(Expression.Call(enumerator, enumeratorType.GetMethod("MoveNext"))), Expression.Break(breakLabel));

            ParameterExpression sourceKeyVar = Expression.Variable(sourceKeyType);
            ParameterExpression sourceValueVar = Expression.Variable(sourceValueType);

            ParameterExpression destinationKeyVar = Expression.Variable(destinationKeyType);
            ParameterExpression destinationValueVar = Expression.Variable(destinationValueType);

            ParameterExpression currentSourcePair = Expression.Variable(sourcePairType);

            Expression assignSourceCurrentPair = Expression.Assign(currentSourcePair, Expression.Property(enumerator, "Current"));
            Expression assignSourceKeyVar = Expression.Assign(sourceKeyVar, Expression.Property(currentSourcePair, "Key"));
            Expression assignSourceValueVar = Expression.Assign(sourceValueVar, Expression.Property(currentSourcePair, "Value"));

            Expression mapKey = ExpressionToMapObjects(sourceKeyVar, sourceKeyType, null, destinationKeyVar, destinationKeyType, null, mapExistingDestination, initialDestinationType, assumeDestinationExplicitly);
            Expression mapValue = ExpressionToMapObjects(sourceValueVar, sourceValueType, null, destinationValueVar, destinationValueType, null, mapExistingDestination, initialDestinationType, assumeDestinationExplicitly);

            if (mapKey.NodeType == ExpressionType.Default || mapValue.NodeType == ExpressionType.Default)
                return Expression.Empty();

            Expression addItemToDestination = Expression.Call(destinationExpr,
                                                              destinationType.GetMethod("Add", new Type[] { destinationKeyType, destinationValueType }),
                                                              new Expression[] { destinationKeyVar, destinationValueVar });

            List<Expression> loop = new List<Expression>(9) { breakLoop, assignSourceCurrentPair, assignSourceKeyVar, assignSourceValueVar, mapKey, mapValue, addItemToDestination };

            if (!destinationKeyType.IsValueType)
                loop.Add(Expression.Assign(destinationKeyVar, Expression.Constant(null, destinationKeyType)));
            if (!destinationValueType.IsValueType)
                loop.Add(Expression.Assign(destinationValueVar, Expression.Constant(null, destinationValueType)));

            Expression[] expressions = new Expression[2];

            expressions[0] = ExpressionToInstantiateDestination(destinationExpr, Expression.Property(sourceExpr, "Count"), destinationType, mapExistingDestination);

            expressions[1] = Expression.Block(new ParameterExpression[] { enumerator, currentSourcePair, sourceKeyVar, sourceValueVar, destinationKeyVar, destinationValueVar },
                                              new Expression[] { assignEnumerator, Expression.Loop(Expression.Block(loop), breakLabel) });

            BinaryExpression sourceNotNull = Expression.NotEqual(sourceExpr, Expression.Constant(null));
            return Expression.IfThen(sourceNotNull, Expression.Block(expressions));
        }

        private static Expression ExpressionToMapCollections(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            MemberExpression sourceLength = Expression.Property(sourceExpr, sourceType.IsArray ? "Length" : "Count");

            ParameterExpression index = Expression.Variable(typeof(int));
            BinaryExpression initIndex = Expression.Assign(index, Expression.Constant(0));
            LabelTarget breakLabel = Expression.Label();
            ConditionalExpression breakLoop = Expression.IfThen(Expression.GreaterThanOrEqual(index, sourceLength), Expression.Break(breakLabel));
            UnaryExpression incrementIndex = Expression.PostIncrementAssign(index);

            Type sourceItemType = sourceType.IsArray ? sourceType.GetElementType() : sourceType.GetGenericArguments()[0];
            Type destinationItemType = destinationType.IsArray ? destinationType.GetElementType() : destinationType.GetGenericArguments()[0];

            ParameterExpression sourceVar = Expression.Variable(sourceItemType);
            ParameterExpression destinationVar = Expression.Variable(destinationItemType);

            MethodInfo mapTo = typeof(Mapper).GetMethod("DeepCopyTo", BindingFlags.Static | BindingFlags.Public).MakeGenericMethod(new Type[] { destinationItemType });

            Expression mapItems = Expression.Assign(destinationVar, Expression.Call(null, mapTo, Expression.Convert(sourceVar, typeof(object)), Expression.Constant(assumeDestinationExplicitly)));

            Expression assignSourceVar = null;

            if (sourceType.IsArray)
                assignSourceVar = Expression.Assign(sourceVar, Expression.ArrayIndex(sourceExpr, index));
            else
                assignSourceVar = Expression.Assign(sourceVar, Expression.Property(sourceExpr, sourceType.GetProperty("Item"), index));

            Expression addItemToDestination = null;
            if (destinationType.IsArray)
                addItemToDestination = Expression.Assign(Expression.ArrayAccess(destinationExpr, index), destinationVar);
            else
                addItemToDestination = Expression.Call(destinationExpr, destinationType.GetMethod("Add", new Type[] { destinationItemType }), destinationVar);

            List<Expression> loop = new List<Expression>(6) { breakLoop, assignSourceVar, mapItems, addItemToDestination };

            if (!destinationItemType.IsValueType)
                loop.Add(Expression.Assign(destinationVar, Expression.Constant(null, destinationItemType)));
            loop.Add(incrementIndex);

            Expression[] expressions = new Expression[2];

            expressions[0] = ExpressionToInstantiateDestination(destinationExpr, sourceLength, destinationType, mapExistingDestination);

            expressions[1] = Expression.Block(new ParameterExpression[] { index, sourceVar, destinationVar },
                                              new Expression[] { initIndex, Expression.Loop(Expression.Block(loop), breakLabel) });

            BinaryExpression sourceNotNull = Expression.NotEqual(sourceExpr, Expression.Constant(null));
            return Expression.IfThen(sourceNotNull, Expression.Block(expressions));
        }

        private static Expression ExpressionToMapAbstractTypes(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            ParameterExpression sourceActualType = Expression.Variable(typeof(Type));
            ParameterExpression destinationActualType = Expression.Variable(typeof(Type));

            List<Expression> expressions = new List<Expression>(3);

            Expression assignSourceActualType = null;

            if (sourceType.IsAbstract || HasInheritedTypes(sourceType))
                assignSourceActualType = Expression.Assign(sourceActualType, Expression.Call(sourceExpr, typeof(object).GetMethod("GetType")));
            else
                assignSourceActualType = Expression.Assign(sourceActualType, Expression.Constant(sourceType, typeof(Type)));

            expressions.Add(assignSourceActualType);

            BinaryExpression sourceNotNull = Expression.NotEqual(sourceExpr, Expression.Constant(null));
            BinaryExpression destinationIsNull = Expression.Equal(destinationExpr, Expression.Constant(null));
            MethodInfo mapTo = typeof(Mapper).GetMethod("MapTo", BindingFlags.Static | BindingFlags.NonPublic);
            MethodInfo map = typeof(Mapper).GetMethod("Map", BindingFlags.Static | BindingFlags.NonPublic);
            Expression callMapTo = null;
            Expression callMap = null;

            Type sourceGenericType = sourceType.IsGenericType ? sourceType.GetGenericTypeDefinition() : null;
            Type destinationGenericType = destinationType.IsGenericType ? destinationType.GetGenericTypeDefinition() : null;

            if (sourceGenericType == typeof(IEnumerable<>))
            {
                if (destinationType.IsArray || (destinationGenericType == typeof(List<>)))
                {
                    callMapTo = Expression.Call(null, mapTo, new Expression[] { sourceExpr, sourceActualType, Expression.Constant(destinationType, typeof(Type)), Expression.Constant(false) });
                    callMap = Expression.Call(null, map, new Expression[] { sourceExpr, sourceActualType, destinationExpr, Expression.Constant(destinationType, typeof(Type)), Expression.Constant(false) });
                }

                if (destinationGenericType == typeof(IEnumerable<>))
                {
                    Expression assignDestinationActualType = Expression.IfThenElse(
                                                               Expression.Property(sourceActualType, "IsArray"),
                                                               Expression.Assign(destinationActualType, Expression.Constant(destinationType.GetGenericArguments()[0].MakeArrayType(), typeof(Type))),
                                                               Expression.Assign(destinationActualType, Expression.Constant(typeof(List<>).MakeGenericType(destinationType.GetGenericArguments()[0]), typeof(Type))));
                    expressions.Add(assignDestinationActualType);

                    callMapTo = Expression.Call(null, mapTo, new Expression[] { sourceExpr, sourceActualType, destinationActualType, Expression.Constant(false) });
                    callMap = Expression.Call(null, map, new Expression[] { sourceExpr, sourceActualType, destinationExpr, destinationActualType, Expression.Constant(false) });
                }

                if (mapExistingDestination)
                {
                    expressions.Add(Expression.IfThenElse(destinationIsNull,
                                                          Expression.Assign(destinationExpr, Expression.Convert(callMapTo, destinationType)),
                                                          callMap));
                }
                else
                    expressions.Add(Expression.Assign(destinationExpr, Expression.Convert(callMapTo, destinationType)));

                return Expression.IfThen(sourceNotNull,
                                         Expression.Block(new ParameterExpression[] { sourceActualType, destinationActualType }, expressions));
            }

            if (HasInheritedTypes(destinationType))
            {
                Type actualInitialDestinationType;
                if (initialDestinationType.Namespace.StartsWith("System") && initialDestinationType.IsGenericType)
                    actualInitialDestinationType = initialDestinationType.GetGenericArguments()[0];
                else
                    actualInitialDestinationType = initialDestinationType;

                MethodInfo assumeDestinationType = typeof(Mapper).GetMethod("AssumeDestinationActualType", BindingFlags.Static | BindingFlags.NonPublic);

                Expression assignDestinationActualType = Expression.Assign(destinationActualType, Expression.Call(null, assumeDestinationType, Expression.Constant(actualInitialDestinationType), sourceActualType));

                expressions.Add(assignDestinationActualType);

                callMapTo = Expression.Call(null, mapTo, new Expression[] { sourceExpr, sourceActualType, destinationActualType, Expression.Constant(true) });
                callMap = Expression.Call(null, map, new Expression[] { sourceExpr, sourceActualType, destinationExpr, destinationActualType, Expression.Constant(true) });
            }
            else
            {
                callMapTo = Expression.Call(null, mapTo, new Expression[] { sourceExpr, sourceActualType, Expression.Constant(destinationType, typeof(Type)), Expression.Constant(false) });
                callMap = Expression.Call(null, map, new Expression[] { sourceExpr, sourceActualType, destinationExpr, Expression.Constant(destinationType, typeof(Type)), Expression.Constant(false) });

            }
            if (mapExistingDestination)
            {
                expressions.Add(Expression.IfThenElse(destinationIsNull,
                                                      Expression.Assign(destinationExpr, Expression.Convert(callMapTo, destinationType)),
                                                      callMap));
            }
            else
                expressions.Add(Expression.Assign(destinationExpr, Expression.Convert(callMapTo, destinationType)));

            return Expression.IfThen(sourceNotNull,
                                    Expression.Block(new ParameterExpression[] { sourceActualType, destinationActualType }, expressions));
        }

        private static Type AssumeDestinationActualType(Type initialDestinationType, Type sourceActualType)
        {
            DictionaryKey<Type, Type> key = new DictionaryKey<Type, Type>(initialDestinationType, sourceActualType);

            Lazy<Type> actualType;

            if (!assumeDestinationActualTypesCache.TryGetValue(key, out actualType))
            {
                actualType = CacheDestinationActualType(key);
            }

            return actualType.Value;
        }

        private static Lazy<Type> CacheDestinationActualType(DictionaryKey<Type, Type> key)
        {
            return assumeDestinationActualTypesCache.GetOrAdd(
                key,
                new Lazy<Type>(() => GetDestinationActualType(key.Item1, key.Item2), true));
        }

        private static Type GetDestinationActualType(Type initialDestinationType, Type sourceActualType)
        {
            Type destinationActualType = null;

            if (sourceActualType.Namespace.StartsWith("System"))
            {
                if (sourceActualType.IsGenericType)
                    destinationActualType = sourceActualType.GetGenericTypeDefinition();
                else
                    destinationActualType = sourceActualType;
            }
            else
                destinationActualType = initialDestinationType.Assembly.GetType(string.Concat(initialDestinationType.Namespace, ".", sourceActualType.Name));

            if (sourceActualType.IsGenericType)
            {
                Type[] sourceGenericArguments = sourceActualType.GetGenericArguments();
                Type[] destinationGenericArguments = new Type[sourceGenericArguments.Length];

                for (int i = 0; i < sourceGenericArguments.Length; i++)
                    destinationGenericArguments[i] = GetDestinationActualType(initialDestinationType, sourceGenericArguments[i]);

                destinationActualType = destinationActualType.MakeGenericType(destinationGenericArguments);
            }

            return destinationActualType;
        }

        private static Expression ExpressionToInstantiateDestination(Expression destination, Expression length, Type type, bool mapExisting)
        {
            Expression newInstance = null;
            Type genericType = type.IsGenericType ? type.GetGenericTypeDefinition() : null;

            if (type.IsArray || genericType == typeof(List<>) || genericType == typeof(Dictionary<,>))
                newInstance = Expression.New(type.GetConstructor(new Type[] { typeof(int) }), new Expression[] { length });
            else
                newInstance = Expression.New(type);

            if (mapExisting)
            {
                Expression ensureCapacity;
                if (type.IsArray)
                {
                    ensureCapacity = Expression.IfThen(Expression.NotEqual(length, Expression.Property(destination, "Length")),
                                        Expression.Assign(destination, newInstance));
                }
                else if (genericType != null && (genericType == typeof(List<>) || genericType.GetInterfaces().Any(i => i.IsGenericType && i.GetGenericTypeDefinition() == typeof(IDictionary<,>))))
                {
                    ensureCapacity = Expression.IfThen(Expression.NotEqual(Expression.Property(destination, "Count"), Expression.Constant(0)),
                                        Expression.Call(destination, type.GetMethod("Clear", Type.EmptyTypes)));
                }
                else
                    ensureCapacity = Expression.Empty();


                return Expression.IfThenElse(Expression.Equal(destination, Expression.Constant(null)),
                    Expression.Assign(destination, newInstance),
                    ensureCapacity);
            }

            return Expression.Assign(destination, newInstance);
        }

        private static List<Expression> ExpressionToMapFields(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            FieldInfo[] sourceFields = sourceType.GetFields();
            FieldInfo[] destinationFields = destinationType.GetFields();

            List<Expression> expressions = new List<Expression>(sourceFields.Length);

            foreach (FieldInfo sourceField in sourceFields)
            {
                FieldInfo destinationField = destinationFields.FirstOrDefault(p => p.Name == sourceField.Name);

                if (destinationField == null)
                    continue;

                expressions.Add(ExpressionToMapObjects(sourceExpr, sourceField.FieldType, sourceField.Name, destinationExpr, destinationField.FieldType, destinationField.Name, mapExistingDestination, initialDestinationType, assumeDestinationExplicitly));
            }

            return expressions;
        }

        private static List<Expression> ExpressionToMapProperties(Expression sourceExpr, Type sourceType, Expression destinationExpr, Type destinationType, bool mapExistingDestination, Type initialDestinationType, bool assumeDestinationExplicitly)
        {
            PropertyInfo[] sourceProps = sourceType.GetProperties();
            PropertyInfo[] destinationProps = destinationType.GetProperties();

            List<Expression> expressions = new List<Expression>(sourceProps.Length);

            foreach (PropertyInfo sourceProp in sourceProps)
            {
                if (!sourceProp.CanRead)
                    continue;

                PropertyInfo destinationProp = destinationProps.FirstOrDefault(p => p.Name == sourceProp.Name);
                if (destinationProp == null || !destinationProp.CanWrite)
                    continue;

                expressions.Add(ExpressionToMapObjects(sourceExpr, sourceProp.PropertyType, sourceProp.Name, destinationExpr, destinationProp.PropertyType, destinationProp.Name, mapExistingDestination, initialDestinationType, assumeDestinationExplicitly));
            }

            return expressions;
        }

        private static bool CanDeepCopyPropertyOrField(Type sourceType, Type destinationType, bool assumeDestinationExplicitly)
        {
            if (sourceType == typeof(string) || sourceType == typeof(object))
                return false;

            if (sourceType.IsAbstract || destinationType.IsAbstract || (!assumeDestinationExplicitly && (HasInheritedTypes(destinationType))))
                return false;

            return sourceType.IsClass && !sourceType.IsArray && !sourceType.Namespace.StartsWith("System");
        }

        private static bool HasInheritedTypes(Type type)
        {
            return type.IsAbstract || (type.IsClass && type.Assembly.GetTypes().Where(x => x != type && type.IsAssignableFrom(x)).Any());
        }
    }

    public struct DictionaryKey<T1, T2> : IEquatable<DictionaryKey<T1, T2>>
    {
        public readonly T1 Item1;
        public readonly T2 Item2;

        public DictionaryKey(T1 item1, T2 item2)
        {
            Item1 = item1;
            Item2 = item2;
        }

        public override int GetHashCode()
        {
            return DictionaryKey.GetHashCode(this);
        }

        public override bool Equals(object obj)
        {
            return DictionaryKey.Equals(this, obj);
        }

        public bool Equals(DictionaryKey<T1, T2> other)
        {
            return DictionaryKey.Equals(this, other);
        }
    }

    public struct DictionaryKey<T1, T2, T3> : IEquatable<DictionaryKey<T1, T2, T3>>
    {
        public readonly T1 Item1;
        public readonly T2 Item2;
        public readonly T3 Item3;

        public DictionaryKey(T1 item1, T2 item2, T3 item3)
        {
            Item1 = item1;
            Item2 = item2;
            Item3 = item3;
        }

        public override int GetHashCode()
        {
            return DictionaryKey.GetHashCode(this);
        }

        public override bool Equals(object obj)
        {
            return DictionaryKey.Equals(this, obj);
        }

        public bool Equals(DictionaryKey<T1, T2, T3> other)
        {
            return DictionaryKey.Equals(this, other);
        }
    }

    internal static class DictionaryKey
    {
        internal static bool Equals<T1, T2>(DictionaryKey<T1, T2> key, object obj)
        {
            if (!(obj is DictionaryKey<T1, T2>))
                return false;

            return Equals(key, (DictionaryKey<T1, T2>)obj);
        }

        internal static bool Equals<T1, T2>(DictionaryKey<T1, T2> keyA, DictionaryKey<T1, T2> keyB)
        {
            return EqualityComparer<T1>.Default.Equals(keyA.Item1, keyB.Item1)
                && EqualityComparer<T2>.Default.Equals(keyA.Item2, keyB.Item2);
        }

        internal static bool Equals<T1, T2, T3>(DictionaryKey<T1, T2, T3> key, object obj)
        {
            if (!(obj is DictionaryKey<T1, T2, T3>))
                return false;

            return Equals(key, (DictionaryKey<T1, T2, T3>)obj);
        }

        internal static bool Equals<T1, T2, T3>(DictionaryKey<T1, T2, T3> keyA, DictionaryKey<T1, T2, T3> keyB)
        {
            return EqualityComparer<T1>.Default.Equals(keyA.Item1, keyB.Item1)
                && EqualityComparer<T2>.Default.Equals(keyA.Item2, keyB.Item2)
                && EqualityComparer<T3>.Default.Equals(keyA.Item3, keyB.Item3);
        }

        internal static int GetHashCode<T1, T2>(DictionaryKey<T1, T2> key)
        {
            return CombineHashCodes(
                EqualityComparer<T1>.Default.GetHashCode(key.Item1),
                EqualityComparer<T2>.Default.GetHashCode(key.Item2));
        }

        internal static int GetHashCode<T1, T2, T3>(DictionaryKey<T1, T2, T3> key)
        {
            return CombineHashCodes(
                EqualityComparer<T1>.Default.GetHashCode(key.Item1),
                EqualityComparer<T2>.Default.GetHashCode(key.Item2),
                EqualityComparer<T3>.Default.GetHashCode(key.Item3));
        }

        //From System.Tuple
        internal static int CombineHashCodes(int h1, int h2)
        {
            return (((h1 << 5) + h1) ^ h2);
        }

        internal static int CombineHashCodes(int h1, int h2, int h3)
        {
            return CombineHashCodes(CombineHashCodes(h1, h2), h3);
        }
    }
}