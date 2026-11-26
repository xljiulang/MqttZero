using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace MqttZero.Tests.Architecture;

public sealed class SessionLockRemovalTests
{
    [Fact(Timeout = 10_000)]
    public void ProductionSourcesContainNoSessionLocksOrMutationLeases()
    {
        CancellationToken cancellationToken =
            TestContext.Current.CancellationToken;
        string repositoryRoot = FindRepositoryRoot();
        List<string> violations = [];
        foreach (string path in Directory.EnumerateFiles(
            Path.Combine(repositoryRoot, "src"),
            "*.cs",
            SearchOption.AllDirectories))
        {
            if (IsGeneratedPath(path))
            {
                continue;
            }

            CompilationUnitSyntax root = CSharpSyntaxTree.ParseText(
                File.ReadAllText(path),
                path: path,
                cancellationToken: cancellationToken)
                .GetCompilationUnitRoot(cancellationToken);
            foreach (MemberAccessExpressionSyntax member in root
                .DescendantNodes()
                .OfType<MemberAccessExpressionSyntax>())
            {
                if (member.Name.Identifier.ValueText == "SyncRoot")
                {
                    violations.Add(
                        $"{path}: uses Session SyncRoot at " +
                        $"{member.GetLocation().GetLineSpan().StartLinePosition.Line + 1}.");
                }
            }

            string text = File.ReadAllText(path);
            if (text.Contains(
                "_sessionMutationLocks",
                StringComparison.Ordinal))
            {
                violations.Add(
                    $"{path}: contains _sessionMutationLocks.");
            }

            foreach (BaseTypeDeclarationSyntax type in root
                .DescendantNodes()
                .OfType<BaseTypeDeclarationSyntax>())
            {
                string name = type.Identifier.ValueText;
                if (name is "SessionMutationLock" or
                    "SessionMutationLease")
                {
                    violations.Add(
                        $"{path}: contains obsolete type {name}.");
                }
            }
        }

        Assert.True(
            violations.Count == 0,
            string.Join(Environment.NewLine, violations));
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
            if (File.Exists(Path.Combine(
                directory.FullName,
                "MqttZero.slnx")))
            {
                return directory.FullName;
            }

            directory = directory.Parent;
        }

        throw new InvalidOperationException(
            "Could not locate the MqttZero repository root.");
    }
}
