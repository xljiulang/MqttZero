using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MqttZero.Tests
{
    public sealed class SourceDocumentationTests
    {
        [Fact]
        public void SourceTypesPropertiesAndMethodsCarrySummaries()
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
                        case BaseTypeDeclarationSyntax type:
                            Check(type, type.Identifier.ValueText, path, violations);
                            break;
                        case MethodDeclarationSyntax method:
                            Check(method, method.Identifier.ValueText, path, violations);
                            break;
                        case ConstructorDeclarationSyntax constructor:
                            Check(
                                constructor,
                                constructor.Identifier.ValueText,
                                path,
                                violations);
                            break;
                        case PropertyDeclarationSyntax property:
                            Check(
                                property,
                                property.Identifier.ValueText,
                                path,
                                violations);
                            break;
                        case IndexerDeclarationSyntax:
                            Check(node, "this[]", path, violations);
                            break;
                        case OperatorDeclarationSyntax @operator:
                            Check(
                                @operator,
                                @operator.OperatorToken.ValueText,
                                path,
                                violations);
                            break;
                        case ConversionOperatorDeclarationSyntax conversion:
                            Check(
                                conversion,
                                conversion.Type.ToString(),
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

        private static void Check(
            SyntaxNode node,
            string name,
            string path,
            List<string> violations)
        {
            bool documented = false;
            bool hasSummary = false;
            foreach (SyntaxTrivia trivia in node.GetLeadingTrivia())
            {
                if (!trivia.IsKind(
                    SyntaxKind.SingleLineDocumentationCommentTrivia))
                {
                    continue;
                }

                documented = true;
                string text = trivia.ToFullString();
                if (text.Contains("<inheritdoc", StringComparison.Ordinal) ||
                    (text.Contains("<summary>", StringComparison.Ordinal) &&
                        !IsEmptySummary(text)))
                {
                    hasSummary = true;
                }
            }

            if (!documented || !hasSummary)
            {
                violations.Add(
                    $"{path}: '{name}' has no summary documentation.");
            }
        }

        private static bool IsEmptySummary(string text)
        {
            int start = text.IndexOf("<summary>", StringComparison.Ordinal) +
                "<summary>".Length;
            int end = text.IndexOf("</summary>", start, StringComparison.Ordinal);
            if (end < 0)
            {
                return true;
            }

            string body = text.Substring(start, end - start);
            foreach (string line in body.Split('\n'))
            {
                string trimmed = line.Trim().TrimStart('/').Trim();
                if (trimmed.Length > 0)
                {
                    return false;
                }
            }

            return true;
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
