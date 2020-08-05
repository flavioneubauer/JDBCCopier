package me.alabor.jdbccopier;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;

import me.alabor.jdbccopier.copier.Copier;
import me.alabor.jdbccopier.copier.CopierTask;
import me.alabor.jdbccopier.copier.factory.CopierFactory;
import me.alabor.jdbccopier.copier.factory.FilterFactory;
import me.alabor.jdbccopier.copier.listener.ConsoleCopierListener;
import me.alabor.jdbccopier.database.Database;
import me.alabor.jdbccopier.database.PostgreSQLDatabase;
import me.alabor.jdbccopier.database.factory.DatabaseFactory;
import me.alabor.jdbccopier.database.meta.Table;
import me.alabor.jdbccopier.ui.WorkerStatusPanel;
import me.alabor.jdbccopier.ui.layout.ListLayout;

public class JDBCCopier {

	public static void main(String[] args) throws InterruptedException, IOException {
		/* Config: */
		// default
		String configPath = "config.properties";
		if (args != null && args.length == 1)
			configPath = args[0];

		Properties properties = loadProperties(configPath);
		String sourceType = properties.getProperty("source.type", "");
		String sourceConnectionString = properties.getProperty("source.connectionString", "");
		String targetType = properties.getProperty("target.type", "");
		String targetConnectionString = properties.getProperty("target.connectionString", "");
		String includes = properties.getProperty("include", "");
		String excludes = properties.getProperty("exclude", "");
		int maxWorkers = new Integer(properties.getProperty("maxworkers", "-1")).intValue();

		FilterFactory filterFactory = new FilterFactory();
		List<String> includeTables = filterFactory.createFilterList(includes);
		List<String> excludeTables = filterFactory.createFilterList(excludes);

		// Check config:
		if (sourceType.length() == 0 || sourceConnectionString.length() == 0 || targetType.length() == 0
				|| targetConnectionString.length() == 0 || maxWorkers == -1) {

			JOptionPane.showMessageDialog(new JFrame(),
					"Error with config.properties! Ensure that at least source.connectionString, target.connectionString\n and maxWorkers is defined.",
					"config.properties", JOptionPane.ERROR_MESSAGE);
			System.exit(0);
		}

		/* Run: */
		try {
			DatabaseFactory databaseFactory = new DatabaseFactory();
			Database sourceDatabase = databaseFactory.createDatabase(sourceType, sourceConnectionString);

			sourceDatabase.connect();

			Queue<Table> pool = new ConcurrentLinkedQueue<Table>(
					sourceDatabase.getTables(includeTables, excludeTables));
			final List<Thread> workers = new ArrayList<Thread>(maxWorkers + 1);
			List<WorkerStatusPanel> statusPanels = new ArrayList<WorkerStatusPanel>(maxWorkers + 1);
			ConsoleCopierListener console = new ConsoleCopierListener(false);

			/* Create Workers & Statuspanels: */
			CopierFactory copierFactory = new CopierFactory(databaseFactory);
			List<Copier> pooledCopiers = copierFactory.createPooledCopiers(sourceType, sourceConnectionString,
					targetType, targetConnectionString, maxWorkers, pool);

			for (Copier copier : pooledCopiers) {
				WorkerStatusPanel statusPanel = new WorkerStatusPanel();

				copier.addCopierListener(statusPanel);
				copier.addCopierListener(console);
				workers.add(new Thread(new CopierTask(copier)));
				statusPanels.add(statusPanel);
			}

			/* Create and show frame: */
			JFrame frame = new JFrame("JDBCCopier");
			frame.setMinimumSize(new Dimension(800, 500));
			frame.setSize(frame.getMinimumSize());
			frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

			final JButton btnStart = new JButton("Start");
			btnStart.addActionListener(new ActionListener() {
				@Override
				public void actionPerformed(ActionEvent e) {
					btnStart.setEnabled(false);

					try {
						Database targetDatabase = databaseFactory.createDatabase(targetType, targetConnectionString);
						boolean isTargetPsql = targetType.toLowerCase().contains("postgres");
						if (isTargetPsql) {
							PostgreSQLDatabase psql = (PostgreSQLDatabase) targetDatabase;
							psql.connect();
							psql.prepareFunctions();
							psql.disableAllTriggers(true);
						}

						for (Thread t : workers)
							t.start();

						for (Thread t : workers)
							t.join();

						if (isTargetPsql) {
							PostgreSQLDatabase psql = (PostgreSQLDatabase) targetDatabase;
							psql.disableAllTriggers(false);
						}

					} catch (Exception ex) {
						ex.printStackTrace();
					}
				}
			});

			JPanel statusContainer = new JPanel(new ListLayout());
			for (WorkerStatusPanel workerStatusPanel : statusPanels) {
				statusContainer.add(workerStatusPanel);
			}

			JTextArea txtProperties = new JTextArea(
					properties.toString().replace(", ", ",\n").replace("{", "").replace("}", ""));
			txtProperties.setSize(800, 300);

			JPanel contentPane = new JPanel(new BorderLayout());
			contentPane.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			contentPane.add(btnStart, BorderLayout.NORTH);

			JPanel info = new JPanel(new BorderLayout());
			info.setBorder(BorderFactory.createEmptyBorder(5, 5, 5, 5));
			info.add(txtProperties, BorderLayout.NORTH);
			info.add(new JScrollPane(statusContainer), BorderLayout.CENTER);

			contentPane.add(info, BorderLayout.CENTER);

			frame.setContentPane(contentPane);
			frame.setVisible(true);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * Loads a specific config file.
	 * 
	 * @param configFile
	 * @return
	 */
	private static Properties loadProperties(String configFile) throws IOException {
		Properties props = new Properties();
		props.load(new FileInputStream(configFile));
		return props;
	}

}
