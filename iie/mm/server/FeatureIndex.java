package iie.mm.server;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.ngram.NGramTokenizer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NGramPhraseQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import java.io.Reader;

public class FeatureIndex {

	public static Directory dir = null;
	public static IndexWriterConfig iwc = null;
	public static IndexWriter writer = null;
	public static Analyzer analyzer = null;
	
	public static IndexReader reader = null;
	public static IndexSearcher searcher = null;
	
	public long reopenTo = 10 * 1000;
	public long reopenTs = System.currentTimeMillis();
	
	public ServerConf conf;
	public int gramSize = 4;
	
	public FeatureIndex(ServerConf conf) throws IOException {
		dir = FSDirectory.open(new File(conf.getFeatureIndexPath()));
		analyzer = new StandardAnalyzer(Version.LUCENE_CURRENT);
		iwc = new IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
		iwc.setOpenMode(OpenMode.CREATE_OR_APPEND);
		writer = new IndexWriter(dir, iwc);
	}
	
	public static class NGramAnalyzer extends Analyzer {
		private int gramSize = 4;
		
		public NGramAnalyzer(int gramSize) {
			this.gramSize = gramSize;
		}
		
		@Override
		protected TokenStreamComponents createComponents(String fieldName, Reader reader) {
			Tokenizer source = new NGramTokenizer(reader, gramSize, gramSize);
			TokenStream filter = new LowerCaseFilter(Version.LUCENE_CURRENT, source);
			return new TokenStreamComponents(source, filter);
		}
	}
	
	/**
	 * 
	 * @param key     feature hash value
	 * @param field   feature name
	 * @param value   set@md5
	 * @return
	 */
	public boolean addObject(String key, String field, String value) {
		boolean r = false;
		
		if (writer != null) {
			Document doc = new Document();
			for (int i = 0; i < 16; i++) {
				doc.add(new StringField(field + "_" + i, key.substring(i, i + 4), Field.Store.YES));
			}
			doc.add(new StringField("objkey", value, Field.Store.YES));
			try {
				writer.addDocument(doc);
				r = true;
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (System.currentTimeMillis() - reopenTs >= reopenTo) {
				try {
					writer.commit();
					searcher = null;
					if (reader != null) {
						reader.close();
						reader = null;
					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				reopenTs = System.currentTimeMillis();
			}
			System.out.println("AddObj " + field + "=" + key + " -> " + value + " r=" + r);
		}
		
		return r;
	}
	
	private final static Comparator<Entry<Integer, Integer>> comp = new Comparator<Entry<Integer, Integer>>() {
		@Override
		public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
			return (o2.getValue() - o1.getValue()) > 0 ? 1 : -1;
		}
	};
	
	/**
	 * 
	 * @param key     feature hash value
	 * @param field   feature name
	 * @return
	 * @throws IOException 
	 */
	public static List<String> getObject(String key, String field, int maxEdits) throws IOException {
		List<String> r = new ArrayList<String>();
		HashMap<Integer, Integer> m = new HashMap<Integer, Integer>();
		
		if (reader == null) {
			reader = DirectoryReader.open(dir);
		}
		if (searcher == null) {
			searcher = new IndexSearcher(reader);
		}

		long beginTs = System.currentTimeMillis();
		for (int i = 0; i < 16; i++) {
			//Query q = new TermQuery(new Term(field + "_" + i, key.substring(i, i + 4)));
			Query q = new FuzzyQuery(new Term(field + "_" + i, key.substring(i, i + 4)), 0);
			ScoreDoc[] hits = searcher.search(q, searcher.getIndexReader().maxDoc()).scoreDocs;
			for (int j = 0; j < hits.length; j++) {
				System.out.println(i + "\t" + j + "\t" + hits[j].doc + "\t" + hits[j].score);
				Integer nr = m.get(hits[j].doc);
				if (nr == null)
					m.put(hits[j].doc, 1);
				else
					m.put(hits[j].doc, nr + 1);
			}
		}
		long endTs = System.currentTimeMillis();
		List<Entry<Integer, Integer>> entries = new LinkedList<Entry<Integer, Integer>>();
		entries.addAll(m.entrySet());

		Collections.sort(entries, comp);
		
		for (Entry<Integer, Integer> en : entries) {
			if (en.getValue() >= 16 - maxEdits) {
				Document doc = searcher.doc(en.getKey());
				String objkey = doc.get("objkey");
				if (objkey != null)
					r.add(objkey);
			}
		}
		
		System.out.println("Search " + field + "=" + key + " maxEdits=" + maxEdits + " -> hits " + 
				r.size() + " objs in " + searcher.getIndexReader().maxDoc() + " objs in "+ (endTs - beginTs) + " ms.");
		return r;
	}
	
	public void close() {
		if (writer != null)
			try {
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
	}
	
	public static void main(String[] args) {
		try {
			String str = "1011011110110110110110001101100111000001100000000011011000101110";
			Analyzer a = new NGramAnalyzer(8);

			TokenStream stream = a.tokenStream("content", new StringReader(str));
			OffsetAttribute offsetAttribute = stream.getAttribute(OffsetAttribute.class);
			CharTermAttribute charTermAttribute = stream.addAttribute(CharTermAttribute.class);

			int n = 0;
			while (stream.incrementToken()) {
				int startOffset = offsetAttribute.startOffset();
				int endOffset = offsetAttribute.endOffset();
				String term = charTermAttribute.toString();
				System.out.println("TERM " + (n++) + ": " + term);
			}
			a.close();
			
			Analyzer b = new NGramAnalyzer(8);

			// Store the index in memory:
			Directory directory = new RAMDirectory();
			// To store an index on disk, use this instead:
			//Directory directory = FSDirectory.open("/tmp/testindex");
			IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_CURRENT, b);
			IndexWriter iwriter = new IndexWriter(directory, config);
			Document doc = new Document();

			doc.add(new Field("phash", str, TextField.TYPE_STORED));
			iwriter.addDocument(doc);
			iwriter.close();

			// Now search the index:
			DirectoryReader ireader = DirectoryReader.open(directory);
			IndexSearcher isearcher = new IndexSearcher(ireader);
			// Parse a simple query that searches for "text":
			//QueryParser parser = new QueryParser(Version.LUCENE_CURRENT, "phash", b);
			String q = "10110110";
			String q1 = "10110111";
			String q2 = "01101111";
			String q3 = "11011110";
			//Query query = parser.parse(q);
			PhraseQuery pq = new NGramPhraseQuery(8);
			pq.add(new Term("phash", q2), 0);
			pq.add(new Term("phash", q3));
			pq.rewrite(ireader);
			
			ScoreDoc[] hits = isearcher.search(pq, null, 1000).scoreDocs;
			// Iterate through the results:
			System.out.println("SRC: " + str);
			System.out.println("QRY: " + q);
			for (int i = 0; i < hits.length; i++) {
				Document hitDoc = isearcher.doc(hits[i].doc);
				System.out.println("HIT: " + hitDoc.get("phash"));
			}
			ireader.close();
			directory.close();
		} catch (IOException ie) {
			System.out.println("IO Error " + ie.getMessage());
		}
	}
}
