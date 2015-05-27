package com.hypermindr.processor.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.hypermindr.processor.tailer.TailerContext;

/**
 * Builds contexts to isolate each tenant configuration, enabling multi-tenant
 * processing.
 * 
 * @author ricardo
 * 
 */
public class TailerContextBuilder {

	private static TailerContextBuilder instance = new TailerContextBuilder();

	public static TailerContextBuilder getInstance() {

		return instance;
	}

	public List<TailerContext> init(File xmlFile) {
		List<TailerContext> listContexts = new ArrayList<TailerContext>();
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;
		try {
			dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(xmlFile);
			NodeList nList = doc.getElementsByTagName("environment");
			for (int temp = 0; temp < nList.getLength(); temp++) {
				TailerContext ctx = new TailerContext();
				Node nNode = nList.item(temp);

				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;
					ctx.setName(eElement.getElementsByTagName("name").item(0)
							.getTextContent());
					ctx.setPerformanceStreamName(eElement
							.getElementsByTagName("performanceStreamName")
							.item(0).getTextContent());
					ctx.setRawStreamName(eElement
							.getElementsByTagName("rawStreamName").item(0)
							.getTextContent());
					ctx.setRawEnabled(eElement
							.getElementsByTagName("rawEnabled").item(0)
							.getTextContent());
					ctx.setTailerBufferSize(eElement
							.getElementsByTagName("tailerBufferSize").item(0)
							.getTextContent());
					ctx.setDebug(eElement.getElementsByTagName("debug").item(0)
							.getTextContent());
					NodeList nodeFiles = eElement.getElementsByTagName("file");
					ArrayList<String> fs = new ArrayList<String>();
					for (int idxFilex = 0; idxFilex < nodeFiles.getLength(); idxFilex++) {
						Node node = nodeFiles.item(idxFilex);
						fs.add(node.getTextContent().replaceAll("\\s+", ""));
					}
					ctx.setLogFilesNames(fs.toArray(new String[fs.size()]));
				}
				listContexts.add(ctx);
			}
		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return listContexts;
	}

	public TailerConfiguration getConfig(File xmlFile)
			throws ParserConfigurationException, SAXException, IOException {
		TailerConfiguration config = new TailerConfiguration();
		DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
		DocumentBuilder dBuilder;

		dBuilder = dbFactory.newDocumentBuilder();
		Document doc = dBuilder.parse(xmlFile);
		NodeList nList = doc.getElementsByTagName("config");
		for (int temp = 0; temp < nList.getLength(); temp++) {

			Node nNode = nList.item(temp);

			if (nNode.getNodeType() == Node.ELEMENT_NODE) {

				Element eElement = (Element) nNode;
				config.setAwssecretkey(eElement
						.getElementsByTagName("awssecretkey").item(0)
						.getTextContent());
				config.setAwsaccesskey(eElement
						.getElementsByTagName("awsaccesskey").item(0)
						.getTextContent());
				config.setAwskinesisCname(eElement
						.getElementsByTagName("awskinesisCname").item(0)
						.getTextContent());
				config.setAwskinesisEndpoint(eElement
						.getElementsByTagName("awskinesisEndpoint").item(0)
						.getTextContent());
				config.setAwskinesisLocation(eElement
						.getElementsByTagName("awskinesisLocation").item(0)
						.getTextContent());
			}

		}

		return config;
	}

}
