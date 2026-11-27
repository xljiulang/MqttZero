using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using System.Text;

namespace MqttZero.Tests
{
    public sealed class SourceLayoutTests
    {
        private const int MaximumSingleLineLength = 200;

        [Fact]
        public void SourceDeclarationsStayOnOneLine()
        {
            string repositoryRoot = FindRepositoryRoot();
            string sourceRoot = Path.Combine(repositoryRoot, "src");
            List<string> violations = [];

            foreach (string path in Directory.EnumerateFiles(
                sourceRoot,
                "*.cs",
                SearchOption.AllDirectories))
            {
                string normalized = path.Replace('\\', '/');
                if (normalized.Contains("/obj/", StringComparison.Ordinal) ||
                    normalized.Contains("/bin/", StringComparison.Ordinal))
                {
                    continue;
                }

                string text = File.ReadAllText(path);
                CompilationUnitSyntax root = CSharpSyntaxTree
                    .ParseText(
                        text,
                        path: path,
                        cancellationToken: TestContext.Current.CancellationToken)
                    .GetCompilationUnitRoot(
                        TestContext.Current.CancellationToken);
                foreach (SyntaxNode node in root.DescendantNodes())
                {
                    switch (node)
                    {
                        case PropertyDeclarationSyntax property:
                            CheckProperty(property, path, violations);
                            break;
                        case FieldDeclarationSyntax field:
                            CheckDeclaration(
                                field.Declaration,
                                field,
                                path,
                                violations);
                            break;
                    }
                }
            }

            Assert.True(
                violations.Count == 0,
                string.Join(
                    Environment.NewLine,
                    violations.Take(400)));
        }

        [Fact]
        public void DetectorRejectsWrappedDeclarations()
        {
            const string source = """
                namespace Sample;

                internal sealed class Example
                {
                    public int Value
                    { get; }

                    private string _name =
                        "value";
                }
                """;
            CompilationUnitSyntax root = CSharpSyntaxTree
                .ParseText(
                    source,
                    cancellationToken: TestContext.Current.CancellationToken)
                .GetCompilationUnitRoot(
                    TestContext.Current.CancellationToken);
            List<string> violations = [];
            foreach (SyntaxNode node in root.DescendantNodes())
            {
                if (node is PropertyDeclarationSyntax property)
                {
                    CheckProperty(property, "sample.cs", violations);
                }
                else if (node is FieldDeclarationSyntax field)
                {
                    CheckDeclaration(
                        field.Declaration,
                        field,
                        "sample.cs",
                        violations);
                }
            }

            Assert.Equal(2, violations.Count);
        }

        private static void CheckProperty(
            PropertyDeclarationSyntax property,
            string path,
            List<string> violations)
        {
            if (property.AccessorList == null)
            {
                return;
            }

            foreach (AccessorDeclarationSyntax accessor in
                property.AccessorList.Accessors)
            {
                if (accessor.Body != null || accessor.ExpressionBody != null)
                {
                    return;
                }
            }

            if (ContainsNewLine(property))
            {
                violations.Add(
                    $"{Describe(path, property)}: auto property spans lines.");
            }
        }

        private static void CheckDeclaration(
            VariableDeclarationSyntax declaration,
            SyntaxNode owner,
            string path,
            List<string> violations)
        {
            string text = owner.ToString();
            int equals = text.IndexOf('=');
            string head = equals < 0
                ? text.Substring(0, text.IndexOf(';'))
                : text.Substring(0, equals);
            if (ContainsNewLine(head))
            {
                violations.Add(
                    $"{Describe(path, owner)}: declaration head spans lines.");
                return;
            }

            if (!ContainsNewLine(text))
            {
                return;
            }

            if (HasBlockInitializer(declaration))
            {
                return;
            }

            int joined = JoinLength(text);
            if (joined <= MaximumSingleLineLength)
            {
                violations.Add(
                    $"{Describe(path, owner)}: declaration fits " +
                    $"{MaximumSingleLineLength} characters but spans lines.");
            }
        }

        private static bool HasBlockInitializer(
            VariableDeclarationSyntax declaration)
        {
            foreach (VariableDeclaratorSyntax declarator in
                declaration.Variables)
            {
                EqualsValueClauseSyntax? initializer = declarator.Initializer;
                if (initializer == null)
                {
                    continue;
                }

                if (initializer.Value.DescendantNodesAndSelf().Any(
                    static node => node is InitializerExpressionSyntax))
                {
                    return true;
                }

                if (initializer.Value.DescendantNodesAndSelf().Any(
                    static node => node is AnonymousFunctionExpressionSyntax
                    {
                        Block: not null
                    }))
                {
                    return true;
                }
            }

            return false;
        }

        private static bool ContainsNewLine(SyntaxNode node)
        {
            return ContainsNewLine(node.ToString());
        }

        private static bool ContainsNewLine(string text)
        {
            return text.Contains('\n');
        }

        private static int JoinLength(string text)
        {
            StringBuilder builder = new();
            bool pendingSpace = false;
            foreach (char current in text)
            {
                if (char.IsWhiteSpace(current))
                {
                    pendingSpace = builder.Length > 0;
                    continue;
                }

                if (pendingSpace)
                {
                    builder.Append(' ');
                    pendingSpace = false;
                }

                builder.Append(current);
            }

            return builder.Length;
        }

        private static string Describe(string path, SyntaxNode node)
        {
            string name = node switch
            {
                PropertyDeclarationSyntax property =>
                    property.Identifier.ValueText,
                FieldDeclarationSyntax field =>
                    field.Declaration.Variables.First().Identifier.ValueText,
                LocalDeclarationStatementSyntax local =>
                    local.Declaration.Variables.First().Identifier.ValueText,
                _ => node.Kind().ToString()
            };
            int line = node.GetLocation()
                .GetLineSpan()
                .StartLinePosition.Line + 1;
            return $"{Path.GetFileName(path)}:{line}: {name}";
        }

        private static string FindRepositoryRoot()
        {
            DirectoryInfo? directory = new(AppContext.BaseDirectory);
            while (directory != null)
            {
                if (File.Exists(Path.Combine(directory.FullName, "MqttZero.slnx")))
                {
                    return directory.FullName;
                }

                directory = directory.Parent;
            }

            throw new InvalidOperationException(
                "Could not locate the repository root.");
        }
    }
}
