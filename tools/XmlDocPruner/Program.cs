// Prunes a generated XML documentation file down to the externally visible API.
//
// The C# compiler emits a <member> entry for anything carrying a /// comment,
// whatever its accessibility. Measured on SqliteWasmBlazor: 103 of 206 entries
// documented internal types. IntelliSense never surfaces those to a consumer,
// so in a package they are inert weight that also publishes internal design
// notes. Source keeps its /// comments — they are what gives the team
// IntelliSense on internal types inside the solution; only the shipped artifact
// is filtered.
//
// Visibility is read from the compiled assembly rather than inferred from
// source, so it stays correct when a refactor changes an accessibility modifier
// without touching the doc comment.
//
//   XmlDocPruner <assembly.dll> <doc.xml>

using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Xml.Linq;

if (args.Length != 2)
{
    Console.Error.WriteLine("usage: XmlDocPruner <assembly.dll> <doc.xml>");
    return 2;
}

var (assemblyPath, xmlPath) = (args[0], args[1]);
if (!File.Exists(assemblyPath) || !File.Exists(xmlPath))
{
    // Nothing to do rather than an error: the build may legitimately not have
    // produced either file yet.
    return 0;
}

var visible = ReadVisibleTypes(assemblyPath);

var doc = XDocument.Load(xmlPath);
var members = doc.Root?.Element("members");
if (members is null) { return 0; }

var total = members.Elements("member").Count();
var dropped = 0;

foreach (var member in members.Elements("member").ToList())
{
    if (!IsVisible((string?)member.Attribute("name") ?? string.Empty, visible))
    {
        member.Remove();
        dropped++;
    }
}

if (dropped > 0)
{
    doc.Save(xmlPath);
    Console.WriteLine($"XmlDocPruner: kept {total - dropped} of {total} entries " +
                      $"in {Path.GetFileName(xmlPath)} (dropped {dropped} non-public).");
}
return 0;

static bool IsVisible(string docId, HashSet<string> visibleTypes)
{
    if (docId.Length < 3) { return true; }

    // Strip the parameter list, then a conversion operator's ~ReturnType. '#'
    // separates the parts of an explicit interface implementation and is not a
    // type boundary, so flatten it to '.' before splitting.
    var body = docId[2..].Split('(')[0].Split('~')[0].Replace('#', '.');

    if (docId[0] == 'T') { return visibleTypes.Contains(body); }

    // Member ids are Namespace.Type.Member — the split point is not marked, so
    // try progressively shorter prefixes until one names a visible type.
    var parts = body.Split('.');
    for (var take = parts.Length - 1; take > 0; take--)
    {
        if (visibleTypes.Contains(string.Join('.', parts[..take]))) { return true; }
    }
    return false;
}

static HashSet<string> ReadVisibleTypes(string assemblyPath)
{
    using var stream = File.OpenRead(assemblyPath);
    using var pe = new PEReader(stream);
    var md = pe.GetMetadataReader();
    var visible = new HashSet<string>(StringComparer.Ordinal);

    string FullName(TypeDefinition type)
    {
        var name = md.GetString(type.Name);
        var declaring = type.GetDeclaringType();
        if (!declaring.IsNil) { return FullName(md.GetTypeDefinition(declaring)) + "." + name; }
        var ns = md.GetString(type.Namespace);
        return string.IsNullOrEmpty(ns) ? name : ns + "." + name;
    }

    bool Visible(TypeDefinition type)
    {
        var visibility = type.Attributes & TypeAttributes.VisibilityMask;
        if (visibility == TypeAttributes.Public) { return true; }
        // A nested public/protected type is only reachable if every type
        // enclosing it is reachable too.
        if (visibility is TypeAttributes.NestedPublic
                       or TypeAttributes.NestedFamily
                       or TypeAttributes.NestedFamORAssem)
        {
            var declaring = type.GetDeclaringType();
            return !declaring.IsNil && Visible(md.GetTypeDefinition(declaring));
        }
        return false;
    }

    foreach (var handle in md.TypeDefinitions)
    {
        var type = md.GetTypeDefinition(handle);
        if (Visible(type)) { visible.Add(FullName(type)); }
    }
    return visible;
}
