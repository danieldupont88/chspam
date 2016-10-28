package modules.history.composer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A sample class that loads an entity and its contexts from a database.
 * <br/><br/>
 * Each record returned by the SQL Query {@link #P_SEQUENCE_QUERY} corresponds to a unique context.
 * The description of the context ({@link Situation}) is read from the table fields, where the field name (or alias)
 * corresponds to the {@link Situation#predicate}, and the field value corresponds to the {@link Situation#value}.
 * The first {@link java.util.Date} field was converted to a {@link TimeDescription} representation.
 * <br/><br/>
 * The {@link Entity#uid} is provided by the application, and the {@link Entity#name} is read from the first {@link java.lang.String} field.
 * The subsequent fields corresponds to the {@link Entity#attributes}.
 */
public class SimpleJdbcSource { 

   
}
