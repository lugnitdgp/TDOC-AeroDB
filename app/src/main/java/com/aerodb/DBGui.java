package com.aerodb;

import com.aerodb.buffer.BufferManager;
import com.aerodb.index.BTreeFile;
import com.aerodb.storage.*;

import javax.swing.*;
import javax.swing.border.EmptyBorder;
import javax.swing.border.LineBorder;
import java.awt.*;

public class DBGui {
    private JFrame frame;
    private JTextArea logArea;
    private JTextField idField, nameField, ageField, searchField;

    // Backend References
    private HeapPage heapPage;
    private BTreeFile index;
    private HeapFile heapFile;
    private BufferManager indexBm;
    private TupleDesc schema;

    // --- COLORS (Dark Theme) ---
    private final Color BG_COLOR = new Color(30, 30, 30);        // Dark Gray
    private final Color PANEL_COLOR = new Color(45, 45, 45);     // Lighter Gray
    private final Color TEXT_COLOR = new Color(220, 220, 220);   // Off-white
    private final Color INPUT_BG = new Color(60, 60, 60);        // Input Field BG
    private final Color BTN_INSERT = new Color(76, 175, 80);     // Green
    private final Color BTN_SEARCH = new Color(33, 150, 243);    // Blue
    private final Color BTN_EXIT = new Color(244, 67, 54);       // Red

    public DBGui(HeapPage heapPage, BTreeFile index, HeapFile heapFile, BufferManager indexBm, TupleDesc schema) {
        this.heapPage = heapPage;
        this.index = index;
        this.heapFile = heapFile;
        this.indexBm = indexBm;
        this.schema = schema;
        initializeUI();
    }

    private void initializeUI() {
        frame = new JFrame("AeroDB Management Console");
        frame.setSize(500, 650);
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.getContentPane().setBackground(BG_COLOR);
        frame.setLayout(new BoxLayout(frame.getContentPane(), BoxLayout.Y_AXIS));

        // 1. Header
        JPanel headerPanel = new JPanel();
        headerPanel.setBackground(BG_COLOR);
        headerPanel.setBorder(new EmptyBorder(20, 0, 10, 0));
        JLabel title = new JLabel("AeroDB Storage Engine");
        title.setFont(new Font("Segoe UI", Font.BOLD, 22));
        title.setForeground(TEXT_COLOR);
        headerPanel.add(title);
        frame.add(headerPanel);

        // 2. INSERT SECTION
        JPanel insertPanel = createSectionPanel("Insert New Record");
        insertPanel.setLayout(new GridLayout(4, 2, 10, 10)); // Grid for inputs

        idField = createStyledField();
        nameField = createStyledField();
        ageField = createStyledField();
        JButton insertBtn = createStyledButton("INSERT DATA", BTN_INSERT);

        addLabel(insertPanel, "ID:");
        insertPanel.add(idField);
        addLabel(insertPanel, "Name:");
        insertPanel.add(nameField);
        addLabel(insertPanel, "Age:");
        insertPanel.add(ageField);
        insertPanel.add(new JLabel("")); // Empty placeholder
        insertPanel.add(insertBtn);

        frame.add(wrapInContainer(insertPanel));

        // 3. SEARCH SECTION
        JPanel searchPanel = createSectionPanel("Search by Index");
        searchPanel.setLayout(new BorderLayout(10, 10));

        searchField = createStyledField();
        JButton searchBtn = createStyledButton("SEARCH ID", BTN_SEARCH);

        JPanel searchInputContainer = new JPanel(new BorderLayout(5, 0));
        searchInputContainer.setBackground(PANEL_COLOR);
        JLabel searchLbl = new JLabel("Enter ID: ");
        searchLbl.setForeground(TEXT_COLOR);
        
        searchInputContainer.add(searchLbl, BorderLayout.WEST);
        searchInputContainer.add(searchField, BorderLayout.CENTER);

        searchPanel.add(searchInputContainer, BorderLayout.CENTER);
        searchPanel.add(searchBtn, BorderLayout.EAST);

        frame.add(wrapInContainer(searchPanel));

        // 4. LOGS SECTION
        JPanel logPanel = createSectionPanel("System Logs");
        logPanel.setLayout(new BorderLayout());
        
        logArea = new JTextArea();
        logArea.setEditable(false);
        logArea.setFont(new Font("Monospaced", Font.PLAIN, 12));
        logArea.setBackground(new Color(20, 20, 20)); // Very dark
        logArea.setForeground(Color.GREEN);           // Matrix style text
        logArea.setBorder(new EmptyBorder(5, 5, 5, 5));
        
        JScrollPane scrollPane = new JScrollPane(logArea);
        scrollPane.setBorder(new LineBorder(new Color(60, 60, 60)));
        scrollPane.setPreferredSize(new Dimension(400, 150));
        logPanel.add(scrollPane, BorderLayout.CENTER);

        frame.add(wrapInContainer(logPanel));

        // 5. FOOTER (Flush & Exit)
        JPanel footerPanel = new JPanel();
        footerPanel.setBackground(BG_COLOR);
        footerPanel.setBorder(new EmptyBorder(10, 20, 20, 20));
        footerPanel.setLayout(new BorderLayout());

        JButton flushBtn = createStyledButton("FLUSH TO DISK & EXIT", BTN_EXIT);
        flushBtn.setPreferredSize(new Dimension(0, 40)); // Taller button
        footerPanel.add(flushBtn, BorderLayout.CENTER);
        
        frame.add(footerPanel);

        // --- ACTIONS ---

        insertBtn.addActionListener(e -> {
            try {
                int id = Integer.parseInt(idField.getText());
                String name = nameField.getText();
                int age = Integer.parseInt(ageField.getText());

                Tuple t = new Tuple(schema);
                t.setField(0, id);
                t.setField(1, name);
                t.setField(2, age);

                int slot = heapPage.insertTuple(t);
                index.insert(id, new RecordId(0, slot));

                log(">> Inserted: [" + name + "] (ID: " + id + ") at Slot " + slot);
                
                idField.setText("");
                nameField.setText("");
                ageField.setText("");
            } catch (Exception ex) {
                log("Error: " + ex.getMessage());
            }
        });

        searchBtn.addActionListener(e -> {
            try {
                int key = Integer.parseInt(searchField.getText());
                long start = System.nanoTime();
                RecordId rid = index.find(key);
                long end = System.nanoTime();

                if (rid != null) {
                    log("Index Hit! (" + (end - start) + "ns)");
                    Tuple t;
                    if (rid.pageId == 0) {
                        t = heapPage.getTuple(rid.slotNumber, schema);
                    } else {
                        // In a real scenario, fetch via buffer pool
                        Page p = heapFile.readPage(rid.pageId); 
                        t = new HeapPage(p).getTuple(rid.slotNumber, schema);
                    }
                    log(">> FOUND: ID=" + t.getField(0) + ", Name=" + t.getField(1)+", Age="+ t.getField(2));
                } else {
                    log(">> ID " + key + " not found.");
                }
            } catch (Exception ex) {
                log("Search Error: " + ex.getMessage());
            }
        });

        flushBtn.addActionListener(e -> {
            log("Flushing data...");
            try {
                heapFile.writePage(heapPage.getPage());
                indexBm.flushAll();
                log("Saved. Exiting...");
                frame.dispose();
                
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        });
    }

    public void show() {
        frame.setVisible(true);
        log("GUI Initialized. AeroDB Ready.");
    }

    // --- HELPER METHODS FOR STYLING ---

    private JPanel wrapInContainer(JPanel inner) {
        JPanel container = new JPanel(new BorderLayout());
        container.setBackground(BG_COLOR);
        container.setBorder(new EmptyBorder(5, 20, 5, 20)); // Outer margins
        container.add(inner, BorderLayout.CENTER);
        return container;
    }

    private JPanel createSectionPanel(String title) {
        JPanel p = new JPanel();
        p.setBackground(PANEL_COLOR);
        p.setBorder(BorderFactory.createCompoundBorder(
            new LineBorder(new Color(60,60,60), 1),
            new EmptyBorder(15, 15, 15, 15)
        ));
        
        // Add Title Border
        p.setBorder(BorderFactory.createTitledBorder(
            new LineBorder(new Color(80,80,80)), 
            title, 
            0, 0, 
            new Font("Segoe UI", Font.BOLD, 12), 
            TEXT_COLOR
        ));
        return p;
    }

    private JTextField createStyledField() {
        JTextField tf = new JTextField();
        tf.setBackground(INPUT_BG);
        tf.setForeground(Color.WHITE);
        tf.setCaretColor(Color.WHITE);
        tf.setBorder(BorderFactory.createCompoundBorder(
            new LineBorder(new Color(100,100,100)),
            new EmptyBorder(5, 5, 5, 5)
        ));
        tf.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        return tf;
    }

    private JButton createStyledButton(String text, Color bg) {
        JButton btn = new JButton(text);
        btn.setBackground(bg);
        btn.setForeground(Color.WHITE);
        btn.setFocusPainted(false);
        btn.setFont(new Font("Segoe UI", Font.BOLD, 12));
        btn.setBorder(new EmptyBorder(8, 15, 8, 15));
        btn.setCursor(new Cursor(Cursor.HAND_CURSOR));
        return btn;
    }

    private void addLabel(JPanel p, String text) {
        JLabel l = new JLabel(text);
        l.setForeground(TEXT_COLOR);
        l.setFont(new Font("Segoe UI", Font.PLAIN, 14));
        p.add(l);
    }

    private void log(String msg) {
        logArea.append(msg + "\n");
        logArea.setCaretPosition(logArea.getDocument().getLength());
    }
}