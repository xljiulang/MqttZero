using MqttZero.Server;
using MqttZero.Server.Broker;
using MqttZero.Server.Interceptors;
using MqttZero.Memory;
using MqttZero.Protocol;
using MqttZero.Server.Routing;
using MqttZero.Server.Sessions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MqttZero.Tests
{
    public sealed class SourceConventionTests
    {
        private static readonly HashSet<string> AllowedServerRootSources =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "MqttZeroOptions.cs"
            };

        private static readonly HashSet<string> AllowedCommonRootSources =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "MqttZeroPayloadOwner.cs",
                "MqttZeroPayloadOwnerFactory.cs",
                "MqttZeroPooledTopicAlias.cs"
            };

        private static readonly Dictionary<string, string> ProjectRootNamespaces =
            new(StringComparer.OrdinalIgnoreCase)
            {
                ["src/MqttZero"] = "MqttZero",
                ["src/MqttZero.Server"] = "MqttZero.Server",
                ["src/MqttZero.AspNetCore"] = "MqttZero.AspNetCore",
                ["src/MqttZero.AspNetCore.Dashboard"] = "MqttZero.AspNetCore.Dashboard",
                ["tests/MqttZero.Tests"] = "MqttZero.Tests",
                ["tests/MqttZero.Dashboard.Tests"] = "MqttZero.Dashboard.Tests",
                ["benchmarks/MqttZero.Benchmarks"] = "MqttZero.Benchmarks",
                ["samples/MqttZero.Sample"] = "MqttZero.Sample"
            };

        private static readonly HashSet<string> NamespaceOptionalFiles =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "Program.cs",
                "TestAssembly.cs"
            };

        private static readonly HashSet<string> AllowedTestRootSources =
            new(StringComparer.OrdinalIgnoreCase)
            {
                "OptionsTests.cs",
                "PayloadOwnerTests.cs",
                "RetainedLeaseTests.cs",
                "SourceConventionTests.cs",
                "SourceDocumentationTests.cs",
                "TestAssembly.cs",
                "TopicAliasAllocationTests.cs"
            };

        [Fact]
        public void SourceFilesFollowTeamConventions()
        {
            string repositoryRoot = FindRepositoryRoot();
            string[] sourceFiles = Directory.EnumerateFiles(
                    repositoryRoot,
                    "*.cs",
                    SearchOption.AllDirectories)
                .Where(static path => !IsGeneratedPath(path))
                .ToArray();
            List<string> violations = [];

            foreach (string path in sourceFiles)
            {
                string text = File.ReadAllText(path);
                CheckLineEndings(path, text, violations);

                SyntaxTree tree = CSharpSyntaxTree.ParseText(
                    text,
                    path: path,
                    cancellationToken: TestContext.Current.CancellationToken);
                CompilationUnitSyntax root = tree.GetCompilationUnitRoot(
                    TestContext.Current.CancellationToken);
                IEnumerable<BaseTypeDeclarationSyntax> topLevelTypes = root
                    .DescendantNodes()
                    .OfType<BaseTypeDeclarationSyntax>()
                    .Where(static type => type.Parent is CompilationUnitSyntax or BaseNamespaceDeclarationSyntax);
                if (topLevelTypes.Count() > 1)
                {
                    violations.Add($"{path}: contains more than one top-level type.");
                }

                CheckBodies(path, root, violations);
            }

            Assert.True(
                violations.Count == 0,
                string.Join(Environment.NewLine, violations));
        }

        [Fact]
        public void ServerProjectRootContainsOnlyApprovedSmallTypes()
        {
            string repositoryRoot = FindRepositoryRoot();
            string serverProject = Path.Combine(
                repositoryRoot,
                "src",
                "MqttZero.Server");
            string[] unexpected = Directory
                .EnumerateFiles(serverProject, "*.cs", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .Where(static name =>
                    name != null &&
                    !AllowedServerRootSources.Contains(name))
                .Order(StringComparer.Ordinal)
                .ToArray()!;

            Assert.True(
                unexpected.Length == 0,
                "Move these sources into responsibility folders: " +
                string.Join(", ", unexpected));
        }

        [Fact]
        public void CommonProjectRootContainsOnlyApprovedSharedTypes()
        {
            string repositoryRoot = FindRepositoryRoot();
            string commonProject = Path.Combine(
                repositoryRoot,
                "src",
                "MqttZero");
            string[] unexpected = Directory
                .EnumerateFiles(commonProject, "*.cs", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .Where(static name =>
                    name != null &&
                    !AllowedCommonRootSources.Contains(name))
                .Order(StringComparer.Ordinal)
                .ToArray()!;

            Assert.True(
                unexpected.Length == 0,
                "Move these shared sources into responsibility folders: " +
                string.Join(", ", unexpected));
        }

        [Fact]
        public void SourceNamespacesMatchProjectDirectories()
        {
            string repositoryRoot = FindRepositoryRoot();
            List<string> violations = [];
            foreach (KeyValuePair<string, string> project in ProjectRootNamespaces)
            {
                string projectDirectory = Path.Combine(
                    repositoryRoot,
                    project.Key.Replace('/', Path.DirectorySeparatorChar));
                foreach (string path in Directory.EnumerateFiles(
                    projectDirectory,
                    "*.cs",
                    SearchOption.AllDirectories))
                {
                    if (IsGeneratedPath(path))
                    {
                        continue;
                    }

                    string text = File.ReadAllText(path);
                    CompilationUnitSyntax root = CSharpSyntaxTree.ParseText(
                        text,
                        path: path,
                        cancellationToken: TestContext.Current.CancellationToken)
                        .GetCompilationUnitRoot(TestContext.Current.CancellationToken);
                    string? actualNamespace = root.Members
                        .OfType<BaseNamespaceDeclarationSyntax>()
                        .Select(static declaration => declaration.Name.ToString())
                        .FirstOrDefault();
                    if (actualNamespace == null &&
                        NamespaceOptionalFiles.Contains(Path.GetFileName(path)))
                    {
                        continue;
                    }

                    string relativeDirectory = Path.GetRelativePath(
                        projectDirectory,
                        Path.GetDirectoryName(path)!);
                    string expectedNamespace = project.Value;
                    if (relativeDirectory != ".")
                    {
                        string namespaceSuffix = relativeDirectory
                            .Replace(Path.DirectorySeparatorChar, '.')
                            .Replace(Path.AltDirectorySeparatorChar, '.');
                        expectedNamespace += "." + namespaceSuffix;
                    }

                    if (!string.Equals(
                        actualNamespace,
                        expectedNamespace,
                        StringComparison.Ordinal))
                    {
                        violations.Add(
                            $"{path}: expected namespace '{expectedNamespace}', " +
                            $"found '{actualNamespace ?? "<none>"}'.");
                    }
                }
            }

            Assert.True(
                violations.Count == 0,
                string.Join(Environment.NewLine, violations));
        }

        [Fact]
        public void TestProjectRootContainsOnlyApprovedSmallTests()
        {
            string repositoryRoot = FindRepositoryRoot();
            string testProject = Path.Combine(
                repositoryRoot,
                "tests",
                "MqttZero.Tests");
            string[] unexpected = Directory
                .EnumerateFiles(testProject, "*.cs", SearchOption.TopDirectoryOnly)
                .Select(Path.GetFileName)
                .Where(static name =>
                    name != null &&
                    !AllowedTestRootSources.Contains(name))
                .Order(StringComparer.Ordinal)
                .ToArray()!;

            Assert.True(
                unexpected.Length == 0,
                "Move these test sources into domain folders: " +
                string.Join(", ", unexpected));
        }

        [Fact]
        public void NoMethodForwardsToSharedWireHelpers()
        {
            string repositoryRoot = FindRepositoryRoot();
            string[] sourceFiles = Directory
                .EnumerateFiles(
                    Path.Combine(repositoryRoot, "src"),
                    "*.cs",
                    SearchOption.AllDirectories)
                .Where(static path => !IsGeneratedPath(path))
                .ToArray();
            List<string> violations = [];
            foreach (string path in sourceFiles)
            {
                CompilationUnitSyntax root = CSharpSyntaxTree.ParseText(
                    File.ReadAllText(path),
                    path: path,
                    cancellationToken: TestContext.Current.CancellationToken)
                    .GetCompilationUnitRoot(TestContext.Current.CancellationToken);
                violations.AddRange(FindWireHelperForwards(path, root));
            }

            Assert.True(
                violations.Count == 0,
                "Call the shared wire helpers directly instead of wrapping them: " +
                Environment.NewLine +
                string.Join(Environment.NewLine, violations));
        }

        [Fact]
        public void WireHelperForwardRuleDetectsInlineForwards()
        {
            const string source = """
                internal static class Sample
                {
                    public static bool Forward(ref int reader, out int value)
                    {
                        return MqttZeroWireFormat.TryReadUInt16(ref reader, out value);
                    }
                }
                """;
            CompilationUnitSyntax root = CSharpSyntaxTree.ParseText(
                source,
                cancellationToken: TestContext.Current.CancellationToken)
                .GetCompilationUnitRoot(TestContext.Current.CancellationToken);

            List<string> violations = FindWireHelperForwards("inline.cs", root);

            Assert.Single(violations);
            Assert.Contains("Forward", violations[0], StringComparison.Ordinal);
        }

        private static List<string> FindWireHelperForwards(
            string path,
            CompilationUnitSyntax root)
        {
            List<string> violations = [];
            foreach (MethodDeclarationSyntax method in root
                .DescendantNodes()
                .OfType<MethodDeclarationSyntax>())
            {
                if (method.Body == null || method.Body.Statements.Count != 1)
                {
                    continue;
                }

                if (method.Body.Statements[0] is not ReturnStatementSyntax
                    {
                        Expression: InvocationExpressionSyntax
                        {
                            Expression: MemberAccessExpressionSyntax memberAccess
                        }
                    })
                {
                    continue;
                }

                string target = memberAccess.Expression switch
                {
                    IdentifierNameSyntax identifier => identifier.Identifier.ValueText,
                    GenericNameSyntax generic => generic.Identifier.ValueText,
                    _ => string.Empty
                };
                if (target is "MqttZeroWireFormat" or "Mqtt5PropertyLayout")
                {
                    violations.Add(
                        $"{path}: method '{method.Identifier.ValueText}' only " +
                        $"forwards to {target}.");
                }
            }

            return violations;
        }

        private static void CheckLineEndings(string path, string text, List<string> violations)
        {
            for (int index = 0; index < text.Length; index++)
            {
                if (text[index] == '\r')
                {
                    if (index + 1 >= text.Length || text[index + 1] != '\n')
                    {
                        violations.Add($"{path}: contains a CR without LF.");
                        return;
                    }

                    index++;
                    continue;
                }

                if (text[index] == '\n')
                {
                    violations.Add($"{path}: contains an LF without CR.");
                    return;
                }
            }
        }

        private static void CheckBodies(
            string path,
            CompilationUnitSyntax root,
            List<string> violations)
        {
            foreach (MethodDeclarationSyntax method in root.DescendantNodes().OfType<MethodDeclarationSyntax>())
            {
                if (method.ExpressionBody != null)
                {
                    violations.Add($"{path}: method '{method.Identifier.ValueText}' uses an expression body.");
                }
            }

            foreach (IfStatementSyntax statement in root.DescendantNodes().OfType<IfStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: an if statement does not use braces.");
                }
            }

            foreach (ForStatementSyntax statement in root.DescendantNodes().OfType<ForStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a for statement does not use braces.");
                }
            }

            foreach (ForEachStatementSyntax statement in root.DescendantNodes().OfType<ForEachStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a foreach statement does not use braces.");
                }
            }

            foreach (WhileStatementSyntax statement in root.DescendantNodes().OfType<WhileStatementSyntax>())
            {
                if (statement.Statement is not BlockSyntax)
                {
                    violations.Add($"{path}: a while statement does not use braces.");
                }
            }
        }

        private static bool IsGeneratedPath(string path)
        {
            string normalized = path.Replace('\\', '/');
            return normalized.Contains("/obj/", StringComparison.Ordinal) ||
                normalized.Contains("/bin/", StringComparison.Ordinal) ||
                normalized.Contains("/artifacts/", StringComparison.Ordinal);
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

            throw new InvalidOperationException("Could not locate the MqttZero repository root.");
        }
    }
}
