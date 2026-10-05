"""Summarises a PIT mutations.xml: totals, the score without Kotlin-generated null-check mutants, per-class counts and
every survivor. Usage: python3 tools/mutation/triage.py <path to mutations.xml>"""
import collections
import sys
import xml.etree.ElementTree as ET


def kotlin_null_check(mutation):
    # Removing a null check Kotlin generates (Intrinsics.checkNotNull*) is unobservable: the values are never null.
    return 'kotlin/jvm/internal/Intrinsics' in mutation['description']


def main(path):
    mutations = []
    for element in ET.parse(path).getroot().findall('mutation'):
        fields = {child.tag: (child.text or '') for child in element}
        indexes = element.find('indexes')
        fields['indexes'] = ','.join(index.text for index in indexes) if indexes is not None else ''
        fields['status'] = element.get('status')
        fields['detected'] = element.get('detected') == 'true'
        mutations.append(fields)

    def score(items):
        detected = sum(1 for m in items if m['detected'])
        return f"{detected}/{len(items)} detected ({100.0 * detected / max(1, len(items)):.1f}%)"

    print('all mutants:            ', score(mutations), dict(collections.Counter(m['status'] for m in mutations)))
    real = [m for m in mutations if not kotlin_null_check(m)]
    print('without Kotlin null checks:', score(real), dict(collections.Counter(m['status'] for m in real)))
    print()
    per_class = collections.defaultdict(collections.Counter)
    for m in real:
        per_class[m['mutatedClass'].split('.')[-1].split('$')[0]]['detected' if m['detected'] else 'survived'] += 1
    for name, counts in sorted(per_class.items()):
        print(f"  {name:38} detected={counts['detected']:3} survived={counts['survived']:3}")
    print()
    print('survivors (without Kotlin null checks):')
    for m in real:
        if not m['detected']:
            print(f"  {m['status']:12} {m['mutatedClass'].split('.')[-1]}.{m['mutatedMethod']} L{m['lineNumber']} "
                  f"[{m['mutator'].split('.')[-1]} @{m['indexes']}] {m['description']}")


if __name__ == '__main__':
    main(sys.argv[1])
