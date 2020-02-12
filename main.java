import javax.swing.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

public class main {

    public static void main(String[] args) throws IOException {

        File file = new File(args[0]);
        PrintWriter writer = new PrintWriter("parsed.xml");

        BufferedReader br =  new BufferedReader(
                new InputStreamReader(
                        new FileInputStream(file)));
        String st;
        br.readLine();
        br.readLine();
        br.readLine();
        int counter =0;
        while ((st = br.readLine()) != null) {
            if (st.contains("</phdthesis>")) {
               st = st.replace("</phdthesis>", "</phdthesis>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
           else if (st.contains("</article>")) {
                st = st.replace("</article>", "</article>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</inproceedings>")) {
                st = st.replace("</inproceedings>", "</inproceedings>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</proceedings>")) {
                st = st.replace("</proceedings>", "</proceedings>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</book>")) {
                st = st.replace("</book>", "</book>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</incollection>")) {
                st = st.replace("</incollection>", "</incollection>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</mastersthesis>")) {
                st = st.replace("</mastersthesis>", "</mastersthesis>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if (st.contains("</www>")) {

                st = st.replace("</www>", "</www>\n");
                if(st.contains("</dblp>"))
                {
                    st = st.replace("</dblp>", "");

                }
            }
            else if(st.contains("</dblp>"))
            {
                st = st.replace("</dblp>", "");

            }
            if(counter>10)
                break;
            writer.print(st);




        }
        writer.close();
        br.close();




    }
}
